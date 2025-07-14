// trpc-driver/main.go
package main

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

// --- Driver Core Data Structures ---

// Command represents a message sent from the client to the broker.
type Command struct {
	Action        string          `json:"action"`
	Topic         string          `json:"topic,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

// Message represents a message received from the broker.
type Message struct {
	Topic         string          `json:"topic"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

// Handler functions for different message types.
type EventHandler func(msg Message)
type RpcHandler func(req Message) (any, error)

// pendingRequest holds the channel to send the response back to a waiting Request call.
type pendingRequest struct {
	responseChan chan Message
	errorChan    chan error
}

// Client is the main driver struct that manages the connection and state.
type Client struct {
	conn net.Conn
	mu   sync.Mutex // Protects handlers and pending maps

	eventHandlers map[string]EventHandler
	rpcHandlers   map[string]RpcHandler
	pendingReqs   map[string]pendingRequest
}

// Connect establishes a connection to the TRPC broker using a URI and returns a new client driver.
// The URI format is: trpc://user:password@host:port OR trpcs://user:password@host:port
func Connect(uri string) (*Client, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid TRPC URI: %w", err)
	}

	var conn net.Conn
	switch parsedURL.Scheme {
	case "trpc":
		conn, err = net.Dial("tcp", parsedURL.Host)
	case "trpcs":
		// For production, you would load a specific CA certificate pool.
		// For this example, we skip verification for self-signed certs.
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.Dial("tcp", parsedURL.Host, tlsConfig)
	default:
		return nil, errors.New("invalid URI scheme, must be 'trpc' or 'trpcs'")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to TRPC broker at %s: %w", parsedURL.Host, err)
	}

	user := parsedURL.User.Username()
	pass, _ := parsedURL.User.Password()

	// Authenticate
	authPayload, _ := json.Marshal(map[string]string{"user": user, "pass": pass})
	authCmd, _ := json.Marshal(Command{Action: "AUTH", Payload: authPayload})
	_, err = conn.Write(append(authCmd, '\n'))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send auth command: %w", err)
	}

	// Create ONE reader for the entire connection lifetime.
	reader := bufio.NewReader(conn)

	// Wait for auth response
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

	client := &Client{
		conn:          conn,
		eventHandlers: make(map[string]EventHandler),
		rpcHandlers:   make(map[string]RpcHandler),
		pendingReqs:   make(map[string]pendingRequest),
	}

	// Start the listener goroutine and pass the SAME reader to it.
	go client.listen(reader)

	return client, nil
}

// --- Private Helper Methods ---

// sendCommand marshals and sends a command to the broker.
func (c *Client) sendCommand(cmd Command) error {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}
	_, err = c.conn.Write(append(cmdBytes, '\n'))
	return err
}

// listen is the main loop for reading messages from the broker.
func (c *Client) listen(reader *bufio.Reader) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			// Check for the specific "use of closed network connection" error.
			// This happens on a clean shutdown, so we can exit silently.
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

// dispatch routes incoming messages to the appropriate handler.
func (c *Client) dispatch(msg Message) {
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
		go handler(msg)
		return
	}
	log.Printf("Received message for unhandled topic: %s", msg.Topic)
}

// handleRpc executes the registered RPC handler and sends a response.
func (c *Client) handleRpc(handler RpcHandler, req Message) {
	responsePayload, err := handler(req)
	if err != nil {
		log.Printf("RPC handler for topic '%s' returned an error: %v", req.Topic, err)
		responsePayload = map[string]string{"error": err.Error()}
	}

	payloadBytes, _ := json.Marshal(responsePayload)
	respCmd := Command{
		Action:        "RESPOND",
		CorrelationID: req.CorrelationID,
		Payload:       payloadBytes,
	}
	if err := c.sendCommand(respCmd); err != nil {
		log.Printf("Failed to send RPC response: %v", err)
	}
}

// --- Public API Methods ---

// Close closes the connection to the broker.
func (c *Client) Close() {
	c.conn.Close()
}

// Publish sends an asynchronous event to a topic.
func (c *Client) Publish(topic string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	return c.sendCommand(Command{Action: "PUBLISH", Topic: topic, Payload: payloadBytes})
}

// Subscribe listens for asynchronous events on a topic.
func (c *Client) Subscribe(topic string, subscriberID string, handler EventHandler) error {
	c.mu.Lock()
	c.eventHandlers[topic] = handler
	c.mu.Unlock()

	payload, _ := json.Marshal(map[string]string{"subscriber_id": subscriberID})
	return c.sendCommand(Command{Action: "SUBSCRIBE", Topic: topic, Payload: payload})
}

// Register declares this client as the handler for an RPC topic.
func (c *Client) Register(topic string, handler RpcHandler) error {
	c.mu.Lock()
	c.rpcHandlers[topic] = handler
	c.mu.Unlock()
	return c.sendCommand(Command{Action: "REGISTER", Topic: topic})
}

// Request performs a synchronous RPC call and waits for a response.
func (c *Client) Request(topic string, payload any, timeout time.Duration) (Message, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal payload: %w", err)
	}

	corrID := uuid.New().String()
	req := pendingRequest{
		responseChan: make(chan Message, 1),
		errorChan:    make(chan error, 1),
	}

	c.mu.Lock()
	c.pendingReqs[corrID] = req
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingReqs, corrID)
		c.mu.Unlock()
	}()

	cmd := Command{
		Action:        "REQUEST",
		Topic:         topic,
		Payload:       payloadBytes,
		CorrelationID: corrID,
	}

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

// --- Example Usage ---

func main() {
	// Change to "trpcs" to test with TLS enabled on the broker.
	brokerURI := "trpc://admin:admin1234@localhost:8772"

	// --- Start the Service Provider (Server) ---
	go func() {
		serverClient, err := Connect(brokerURI)
		if err != nil {
			log.Fatalf("Server client failed to connect: %v", err)
		}
		defer serverClient.Close()

		getUserHandler := func(req Message) (any, error) {
			log.Printf("[Server] Received RPC request for GetUserProfile: %s", string(req.Payload))
			return map[string]any{
				"user_id": "u-123", "name": "Alice", "email": "alice@example.com",
			}, nil
		}
		err = serverClient.Register("user.v1.UserService.GetUserProfile", getUserHandler)
		if err != nil {
			log.Fatalf("Server could not register handler: %v", err)
		}
		log.Println("[Server] Successfully registered GetUserProfile handler.")
		select {}
	}()

	// --- Start two subscribers with the SAME subscriber_id to test load balancing ---
	for i := 1; i <= 2; i++ {
		instanceNum := i
		go func() {
			subClient, err := Connect(brokerURI)
			if err != nil {
				log.Fatalf("[Sub-%d] client failed to connect: %v", instanceNum, err)
			}
			defer subClient.Close()

			eventHandler := func(msg Message) {
				log.Printf("[Sub-%d] Received event on topic '%s': %s", instanceNum, msg.Topic, string(msg.Payload))
			}
			err = subClient.Subscribe("user.v1.UserService.UserCreated", "email-service-group", eventHandler)
			if err != nil {
				log.Fatalf("[Sub-%d] could not subscribe: %v", instanceNum, err)
			}
			log.Printf("[Sub-%d] Successfully subscribed to UserCreated events.", instanceNum)
			select {}
		}()
	}

	// Wait a moment for the server and subscribers to register.
	time.Sleep(2 * time.Second)

	// --- Start the Requester (Client) in its own goroutine ---
	go func() {
		log.Println("[Client] Starting requester...")
		requesterClient, err := Connect(brokerURI)
		if err != nil {
			log.Fatalf("Requester client failed to connect: %v", err)
		}
		defer requesterClient.Close()

		log.Println("[Client] Sending synchronous request to GetUserProfile...")
		requestPayload := map[string]string{"user_id": "u-123"}
		response, err := requesterClient.Request("user.v1.UserService.GetUserProfile", requestPayload, 5*time.Second)
		if err != nil {
			log.Fatalf("[Client] Request failed: %v", err)
		}
		log.Printf("[Client] Received RPC response: %s", string(response.Payload))

		for i := 0; i < 4; i++ {
			log.Printf("[Client] Publishing UserCreated event #%d...", i+1)
			eventPayload := map[string]any{"user_id": fmt.Sprintf("u-new-%d", i), "name": "New User"}
			err = requesterClient.Publish("user.v1.UserService.UserCreated", eventPayload)
			if err != nil {
				log.Fatalf("[Client] Failed to publish event: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		log.Println("[Client] Finished tasks, staying connected.")
	}()

	log.Println("Example services are running. Press Ctrl+C to exit.")
	select {}
}
