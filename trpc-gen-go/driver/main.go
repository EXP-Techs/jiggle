package driver

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// --- Public Driver Interfaces and Structs ---

type Command struct {
	Action        string          `json:"action"`
	Topic         string          `json:"topic,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	MessageID     string          `json:"message_id,omitempty"`
}

type Message struct {
	MessageID     string          `json:"message_id"`
	Topic         string          `json:"topic"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

type EventHandler func(msg Message) error
type RpcHandler func(req Message) (any, error)

type ClientDriver interface {
	Request(topic string, payload any, timeout time.Duration) (Message, error)
	Publish(topic string, payload any) error
	Subscribe(topic string, subscriberID string, handler EventHandler) error
	Register(topic string, handler RpcHandler) error
	Close()
}

// --- Internal Implementation ---

type pendingRequest struct {
	responseChan chan Message
	errorChan    chan error
}

type client struct {
	conn net.Conn
	mu   sync.Mutex

	eventHandlers map[string]EventHandler
	rpcHandlers   map[string]RpcHandler
	pendingReqs   map[string]pendingRequest
}

func Connect(uri string) (ClientDriver, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid TRPC URI: %w", err)
	}
	var conn net.Conn
	switch parsedURL.Scheme {
	case "trpc":
		conn, err = net.Dial("tcp", parsedURL.Host)
	case "trpcs":
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		conn, err = tls.Dial("tcp", parsedURL.Host, tlsConfig)
	default:
		return nil, errors.New("invalid URI scheme, must be 'trpc' or 'trpcs'")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TRPC broker at %s: %w", parsedURL.Host, err)
	}

	user := parsedURL.User.Username()
	pass, _ := parsedURL.User.Password()
	authPayload, _ := json.Marshal(map[string]string{"user": user, "pass": pass})
	authCmd, _ := json.Marshal(Command{Action: "AUTH", Payload: authPayload})
	_, err = conn.Write(append(authCmd, '\n'))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send auth command: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read auth response: %w", err)
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(line, &resp); err != nil || resp.Status != "ok" {
		conn.Close()
		return nil, errors.New("authentication failed")
	}

	c := &client{
		conn:          conn,
		eventHandlers: make(map[string]EventHandler),
		rpcHandlers:   make(map[string]RpcHandler),
		pendingReqs:   make(map[string]pendingRequest),
	}
	go c.listen(reader)
	return c, nil
}

func (c *client) sendCommand(cmd Command) error {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}
	_, err = c.conn.Write(append(cmdBytes, '\n'))
	return err
}

func (c *client) listen(reader *bufio.Reader) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && strings.Contains(opErr.Err.Error(), "use of closed network connection") {
				log.Printf("Connection closed for %s. Listener exiting.", c.conn.RemoteAddr())
			} else if err != io.EOF {
				log.Printf("Connection error for %s: %v. Closing listener.", c.conn.RemoteAddr(), err)
			}
			c.conn.Close()
			return
		}
		var msg Message
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("Could not unmarshal message from broker: %v", err)
			continue
		}
		c.dispatch(msg)
	}
}

func (c *client) dispatch(msg Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pending, ok := c.pendingReqs[msg.CorrelationID]; ok {
		pending.responseChan <- msg
		delete(c.pendingReqs, msg.CorrelationID)
		return
	}
	if handler, ok := c.rpcHandlers[msg.Topic]; ok {
		go c.handleRpc(handler, msg)
		return
	}
	if handler, ok := c.eventHandlers[msg.Topic]; ok {
		go func() {
			err := handler(msg)
			ackCmd := Command{Action: "ACK", MessageID: msg.MessageID, Topic: msg.Topic}
			if err != nil {
				log.Printf("Event handler for topic '%s' failed: %v. Sending NACK.", msg.Topic, err)
				ackCmd.Action = "NACK"
			}
			if err := c.sendCommand(ackCmd); err != nil {
				log.Printf("Failed to send ACK/NACK for message %s: %v", msg.MessageID, err)
			}
		}()
		return
	}
	log.Printf("Received message for unhandled topic: %s", msg.Topic)
}

func (c *client) handleRpc(handler RpcHandler, req Message) {
	responsePayload, err := handler(req)
	if err != nil {
		log.Printf("RPC handler for topic '%s' returned an error: %v", req.Topic, err)
		responsePayload = map[string]string{"error": err.Error()}
	}
	payloadBytes, _ := json.Marshal(responsePayload)
	respCmd := Command{Action: "RESPOND", CorrelationID: req.CorrelationID, Payload: payloadBytes}
	if err := c.sendCommand(respCmd); err != nil {
		log.Printf("Failed to send RPC response: %v", err)
	}
}

func (c *client) Close() { c.conn.Close() }

func (c *client) Publish(topic string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	return c.sendCommand(Command{Action: "PUBLISH", Topic: topic, Payload: payloadBytes})
}

func (c *client) Subscribe(topic string, subscriberID string, handler EventHandler) error {
	c.mu.Lock()
	c.eventHandlers[topic] = handler
	c.mu.Unlock()
	payload, _ := json.Marshal(map[string]string{"subscriber_id": subscriberID})
	return c.sendCommand(Command{Action: "SUBSCRIBE", Topic: topic, Payload: payload})
}

func (c *client) Register(topic string, handler RpcHandler) error {
	c.mu.Lock()
	c.rpcHandlers[topic] = handler
	c.mu.Unlock()
	return c.sendCommand(Command{Action: "REGISTER", Topic: topic})
}

func (c *client) Request(topic string, payload any, timeout time.Duration) (Message, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal payload: %w", err)
	}
	corrID := uuid.New().String()
	req := pendingRequest{responseChan: make(chan Message, 1), errorChan: make(chan error, 1)}
	c.mu.Lock()
	c.pendingReqs[corrID] = req
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingReqs, corrID)
		c.mu.Unlock()
	}()
	cmd := Command{Action: "REQUEST", Topic: topic, Payload: payloadBytes, CorrelationID: corrID}
	if err := c.sendCommand(cmd); err != nil {
		return Message{}, err
	}
	select {
	case res := <-req.responseChan:
		return res, nil
	case err := <-req.errorChan:
		return Message{}, err
	case <-time.After(timeout):
		return Message{}, errors.New("request timed out")
	}
}
