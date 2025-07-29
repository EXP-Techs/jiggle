// main.go
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	walFile             = "trpc_broker.wal"
	redisConfigKey      = "trpc:config"
	redisUsersKey       = "trpc:users"
	redisTopicConfigKey = "trpc:config:topics"
	redisMessagesPrefix = "trpc:messages:"
	ackTimeout          = 30 * time.Second // Time before an unacked message is considered timed out
)

var (
	// Prometheus Metrics
	connectedClients = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "trpc_connected_clients_total",
			Help: "Total number of currently connected clients.",
		},
	)
	messagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trpc_messages_published_total",
			Help: "Total number of published messages by topic.",
		},
		[]string{"topic"},
	)
	rpcRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trpc_rpc_requests_total",
			Help: "Total number of RPC requests by topic.",
		},
		[]string{"topic"},
	)
	messagesAcked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trpc_messages_acked_total",
			Help: "Total number of acknowledged messages by topic.",
		},
		[]string{"topic"},
	)
	messagesNacked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trpc_messages_nacked_total",
			Help: "Total number of not-acknowledged messages by topic.",
		},
		[]string{"topic"},
	)
)

func init() {
	prometheus.MustRegister(connectedClients)
	prometheus.MustRegister(messagesPublished)
	prometheus.MustRegister(rpcRequests)
	prometheus.MustRegister(messagesAcked)
	prometheus.MustRegister(messagesNacked)
}

// --- GUI Hub for WebSockets ---
type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		clients:    make(map[*websocket.Conn]bool),
	}
}
func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()
		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.Lock()
			for conn := range h.clients {
				if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
					slog.Error("failed to broadcast to websocket", "error", err)
				}
			}
			h.mu.Unlock()
		}
	}
}

// --- Core Data Structures ---
type TopicConfig struct {
	DLQTopic       string `json:"dlqTopic"`
	InitialBackoff int    `json:"initialBackoff"` // in ms
	MaxRetries     int    `json:"maxRetries"`
}

type Config struct {
	MaxRetries     int
	InitialBackoff time.Duration
	Users          map[string]string // username -> password
	TopicConfigs   map[string]TopicConfig
	TLSEnabled     bool   // Flag to enable/disable TLS
	CertFile       string // Path to TLS certificate file
	KeyFile        string // Path to TLS private key file
}

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

type UnackedMessage struct {
	Message
	SentAt time.Time
	Client *Client
}

type Client struct {
	conn net.Conn
	id   string
}

type Subscription struct {
	Topic        string
	SubscriberID string
}

type Broker struct {
	listener net.Listener
	clients  map[string]*Client
	mu       sync.RWMutex
	config   Config
	rdb      *redis.Client
	ctx      context.Context
	hub      *Hub
	logger   *slog.Logger

	eventSubs       map[string]map[string][]*Client
	eventSubIndex   map[string]map[string]int
	clientSubs      map[string][]Subscription
	rpcHandlers     map[string][]*Client
	rpcHandlerIndex map[string]int
	pendingReqs     map[string]*Client
	unackedMessages map[string]UnackedMessage
	walFile         *os.File
}

func NewBroker(config Config, rdb *redis.Client, ctx context.Context, hub *Hub, logger *slog.Logger) (*Broker, error) {
	wal, err := os.OpenFile(walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not open WAL file: %w", err)
	}

	return &Broker{
		clients:         make(map[string]*Client),
		config:          config,
		rdb:             rdb,
		ctx:             ctx,
		hub:             hub,
		logger:          logger,
		eventSubs:       make(map[string]map[string][]*Client),
		eventSubIndex:   make(map[string]map[string]int),
		clientSubs:      make(map[string][]Subscription),
		rpcHandlers:     make(map[string][]*Client),
		rpcHandlerIndex: make(map[string]int),
		pendingReqs:     make(map[string]*Client),
		unackedMessages: make(map[string]UnackedMessage),
		walFile:         wal,
	}, nil
}

// --- Broker Methods ---

func (b *Broker) Start(addr string) error {
	var l net.Listener
	var err error

	if b.config.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(b.config.CertFile, b.config.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load server certificate and key: %w", err)
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
		l, err = tls.Listen("tcp", addr, tlsConfig)
		if err != nil {
			return err
		}
		b.logger.Info("TRPC Broker listening securely with TLS", "address", addr)
	} else {
		l, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		b.logger.Info("TRPC Broker listening (insecure)", "address", addr)
	}

	b.listener = l

	go b.unackedMessageJanitor()

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				b.logger.Info("TCP listener closed.")
				return nil
			}
			b.logger.Error("failed to accept connection", "error", err)
			continue
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) unackedMessageJanitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			for msgID, unackedMsg := range b.unackedMessages {
				if time.Since(unackedMsg.SentAt) > ackTimeout {
					b.logger.Warn("message timed out, NACKing", "message_id", msgID, "topic", unackedMsg.Topic)
					delete(b.unackedMessages, msgID)
					go b.handleNack(unackedMsg.Client, Command{MessageID: msgID, Topic: unackedMsg.Topic})
				}
			}
			b.mu.Unlock()
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) Shutdown() {
	b.logger.Info("shutting down broker")
	b.listener.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, client := range b.clients {
		client.conn.Close()
	}
	b.walFile.Close()
}
func (b *Broker) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := b.authenticate(conn, reader); err != nil {
		b.logger.Warn("authentication failed", "client_addr", conn.RemoteAddr().String(), "error", err)
		return
	}
	b.logger.Info("client authenticated", "client_addr", conn.RemoteAddr().String())
	clientID := conn.RemoteAddr().String()
	client := &Client{conn: conn, id: clientID}
	b.mu.Lock()
	b.clients[clientID] = client
	b.mu.Unlock()
	connectedClients.Inc()
	b.broadcastState()
	b.logger.Info("client connected", "client_id", clientID)
	defer func() {
		b.cleanupClient(client)
		b.logger.Info("client disconnected", "client_id", clientID)
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				b.logger.Error("error reading from client", "client_id", clientID, "error", err)
			}
			return
		}
		b.processCommand(client, line)
	}
}
func (b *Broker) authenticate(conn net.Conn, reader *bufio.Reader) error {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("did not receive auth command in time: %w", err)
	}
	var cmd Command
	if err := json.Unmarshal(line, &cmd); err != nil || cmd.Action != "AUTH" {
		return errors.New("expected AUTH command as first message")
	}
	var authPayload struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}
	if err := json.Unmarshal(cmd.Payload, &authPayload); err != nil {
		return errors.New("invalid auth payload")
	}
	b.mu.RLock()
	password, ok := b.config.Users[authPayload.User]
	b.mu.RUnlock()
	if !ok || password != authPayload.Pass {
		return errors.New("invalid credentials")
	}
	resp := `{"status":"ok"}` + "\n"
	_, err = conn.Write([]byte(resp))
	return err
}
func (b *Broker) cleanupClient(client *Client) {
	b.mu.Lock()
	delete(b.clients, client.id)
	connectedClients.Dec()
	if subs, ok := b.clientSubs[client.id]; ok {
		for _, subInfo := range subs {
			if topicSubs, topicExists := b.eventSubs[subInfo.Topic]; topicExists {
				if group, groupExists := topicSubs[subInfo.SubscriberID]; groupExists {
					newGroup := make([]*Client, 0, len(group)-1)
					for _, c := range group {
						if c.id != client.id {
							newGroup = append(newGroup, c)
						}
					}
					if len(newGroup) == 0 {
						delete(topicSubs, subInfo.SubscriberID)
					} else {
						topicSubs[subInfo.SubscriberID] = newGroup
					}
				}
				if len(b.eventSubs[subInfo.Topic]) == 0 {
					delete(b.eventSubs, subInfo.Topic)
				}
			}
		}
		delete(b.clientSubs, client.id)
	}
	for topic, handlers := range b.rpcHandlers {
		newHandlers := make([]*Client, 0, len(handlers)-1)
		for _, handler := range handlers {
			if handler.id != client.id {
				newHandlers = append(newHandlers, handler)
			}
		}
		if len(newHandlers) == 0 {
			delete(b.rpcHandlers, topic)
			delete(b.rpcHandlerIndex, topic)
		} else {
			b.rpcHandlers[topic] = newHandlers
		}
	}
	b.mu.Unlock()
	b.broadcastState()
}
func (b *Broker) getTopicConfig(topic string) TopicConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if cfg, ok := b.config.TopicConfigs[topic]; ok {
		return cfg
	}
	return TopicConfig{
		DLQTopic:       fmt.Sprintf("dlq.%s", topic),
		InitialBackoff: int(b.config.InitialBackoff.Milliseconds()),
		MaxRetries:     b.config.MaxRetries,
	}
}
func (b *Broker) sendMessage(client *Client, msg Message) error {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		b.logger.Error("could not marshal message", "error", err)
		return err
	}
	msgBytes = append(msgBytes, '\n')
	_, err = client.conn.Write(msgBytes)
	return err
}

func (b *Broker) processCommand(client *Client, rawCmd []byte) {
	b.mu.Lock()
	if _, err := b.walFile.Write(rawCmd); err != nil {
		b.logger.Error("critical: failed to write to WAL", "error", err)
	}
	b.mu.Unlock()
	var cmd Command
	if err := json.Unmarshal(rawCmd, &cmd); err != nil {
		b.logger.Warn("failed to unmarshal command", "client_id", client.id, "error", err)
		return
	}
	b.logger.Debug("received command", "action", cmd.Action, "topic", cmd.Topic, "client_id", client.id)
	switch cmd.Action {
	case "SUBSCRIBE":
		b.handleSubscribe(client, cmd)
	case "REGISTER":
		b.handleRegister(client, cmd)
	case "PUBLISH":
		b.handlePublish(client, cmd)
	case "REQUEST":
		b.handleRequest(client, cmd)
	case "RESPOND":
		b.handleRespond(client, cmd)
	case "ACK":
		b.handleAck(client, cmd)
	case "NACK":
		b.handleNack(client, cmd)
	default:
		b.logger.Warn("unknown command action", "action", cmd.Action)
	}
}
func (b *Broker) publishInternal(topic string, payload json.RawMessage) {
	messagesPublished.WithLabelValues(topic).Inc()
	subscriberGroups, ok := b.eventSubs[topic]
	if !ok || len(subscriberGroups) == 0 {
		redisKey := redisMessagesPrefix + topic
		msgToStore, _ := json.Marshal(Message{Topic: topic, Payload: payload})
		if err := b.rdb.LPush(b.ctx, redisKey, msgToStore).Err(); err != nil {
			b.logger.Error("critical: failed to store message in Redis", "topic", topic, "error", err)
		} else {
			b.logger.Info("no subscribers for topic, stored message in Redis", "topic", topic)
		}
		go b.broadcastState()
		return
	}
	msg := Message{
		MessageID: uuid.New().String(),
		Topic:     topic,
		Payload:   payload,
	}
	for subID, clients := range subscriberGroups {
		if len(clients) == 0 {
			continue
		}
		if b.eventSubIndex[topic] == nil {
			b.eventSubIndex[topic] = make(map[string]int)
		}
		idx := b.eventSubIndex[topic][subID]
		selectedClient := clients[idx]
		b.eventSubIndex[topic][subID] = (idx + 1) % len(clients)
		b.logger.Debug("load-balancing event", "topic", topic, "group", subID, "client_id", selectedClient.id)

		b.unackedMessages[msg.MessageID] = UnackedMessage{Message: msg, SentAt: time.Now(), Client: selectedClient}
		if err := b.sendMessage(selectedClient, msg); err != nil {
			b.logger.Warn("failed to send initial message", "client_id", selectedClient.id, "error", err)
		}
	}
}
func (b *Broker) handleSubscribe(client *Client, cmd Command) {
	b.mu.Lock()
	var payload struct {
		SubscriberID string `json:"subscriber_id"`
	}
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.SubscriberID == "" {
		b.logger.Warn("invalid SUBSCRIBE command", "client_id", client.id, "error", "missing subscriber_id")
		b.mu.Unlock()
		return
	}
	if b.eventSubs[cmd.Topic] == nil {
		b.eventSubs[cmd.Topic] = make(map[string][]*Client)
	}
	b.eventSubs[cmd.Topic][payload.SubscriberID] = append(b.eventSubs[cmd.Topic][payload.SubscriberID], client)
	b.clientSubs[client.id] = append(b.clientSubs[client.id], Subscription{
		Topic:        cmd.Topic,
		SubscriberID: payload.SubscriberID,
	})
	b.logger.Info("client subscribed", "client_id", client.id, "topic", cmd.Topic, "subscriber_id", payload.SubscriberID)
	b.mu.Unlock()
	b.broadcastState()
}
func (b *Broker) handleRegister(client *Client, cmd Command) {
	b.mu.Lock()
	b.rpcHandlers[cmd.Topic] = append(b.rpcHandlers[cmd.Topic], client)
	b.logger.Info("client registered as RPC handler", "client_id", client.id, "topic", cmd.Topic, "total_handlers", len(b.rpcHandlers[cmd.Topic]))
	b.mu.Unlock()
	b.broadcastState()
}
func (b *Broker) handlePublish(client *Client, cmd Command) {
	b.mu.Lock()
	b.publishInternal(cmd.Topic, cmd.Payload)
	b.mu.Unlock()
}
func (b *Broker) handleRequest(client *Client, cmd Command) {
	b.mu.Lock()
	rpcRequests.WithLabelValues(cmd.Topic).Inc()
	handlers, ok := b.rpcHandlers[cmd.Topic]
	if !ok || len(handlers) == 0 {
		b.logger.Warn("no handler for RPC topic", "topic", cmd.Topic)
		b.mu.Unlock()
		return
	}
	idx := b.rpcHandlerIndex[cmd.Topic]
	handler := handlers[idx]
	b.rpcHandlerIndex[cmd.Topic] = (idx + 1) % len(handlers)
	b.pendingReqs[cmd.CorrelationID] = client
	b.mu.Unlock()
	msg := Message{
		Topic:         cmd.Topic,
		Payload:       cmd.Payload,
		CorrelationID: cmd.CorrelationID,
	}
	if err := b.sendMessage(handler, msg); err != nil {
		b.logger.Warn("failed to send request", "handler_id", handler.id, "error", err)
	}
	b.logger.Debug("forwarded REQUEST", "topic", cmd.Topic, "handler_id", handler.id)
}
func (b *Broker) handleRespond(client *Client, cmd Command) {
	b.mu.Lock()
	originalClient, ok := b.pendingReqs[cmd.CorrelationID]
	if !ok {
		b.logger.Warn("received RESPONSE for unknown correlation ID", "correlation_id", cmd.CorrelationID)
		b.mu.Unlock()
		return
	}
	delete(b.pendingReqs, cmd.CorrelationID)
	b.mu.Unlock()
	msg := Message{
		Topic:         "response",
		Payload:       cmd.Payload,
		CorrelationID: cmd.CorrelationID,
	}
	if err := b.sendMessage(originalClient, msg); err != nil {
		b.logger.Warn("failed to send response", "client_id", originalClient.id, "error", err)
	}
	b.logger.Debug("forwarded RESPONSE", "correlation_id", cmd.CorrelationID, "client_id", originalClient.id)
}
func (b *Broker) handleAck(client *Client, cmd Command) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.unackedMessages[cmd.MessageID]; ok {
		messagesAcked.WithLabelValues(cmd.Topic).Inc()
		delete(b.unackedMessages, cmd.MessageID)
		b.logger.Debug("message acknowledged", "message_id", cmd.MessageID)
	} else {
		b.logger.Warn("received ACK for unknown or already acked message", "message_id", cmd.MessageID)
	}
}
func (b *Broker) handleNack(client *Client, cmd Command) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if unacked, ok := b.unackedMessages[cmd.MessageID]; ok {
		messagesNacked.WithLabelValues(cmd.Topic).Inc()
		delete(b.unackedMessages, cmd.MessageID)
		b.logger.Warn("message NACKed, moving to retry/DLQ", "message_id", cmd.MessageID, "topic", cmd.Topic)
		go b.publishToDLQ(unacked.Message, b.getTopicConfig(unacked.Topic))
	} else {
		b.logger.Warn("received NACK for unknown or already acked message", "message_id", cmd.MessageID)
	}
}

func (b *Broker) publishToDLQ(msg Message, cfg TopicConfig) {
	dlqTopic := cfg.DLQTopic
	b.logger.Info("republishing failed message to DLQ topic", "dlq_topic", dlqTopic)
	b.mu.Lock()
	b.publishInternal(dlqTopic, msg.Payload)
	b.mu.Unlock()
}

// --- GUI State Broadcasting ---
type GuiSubscriberGroup struct {
	ID      string `json:"id"`
	Clients int    `json:"clients"`
}
type GuiEventTopicInfo struct {
	Groups             []GuiSubscriberGroup `json:"groups"`
	StoredMessageCount int64                `json:"storedMessageCount"`
}
type GuiState struct {
	ConnectedClients int                          `json:"connectedClients"`
	EventTopics      map[string]GuiEventTopicInfo `json:"eventTopics"`
	RpcTopics        map[string]int               `json:"rpcTopics"`
	TopicConfigs     map[string]TopicConfig       `json:"topicConfigs"`
}

func (b *Broker) broadcastState() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	state := GuiState{
		ConnectedClients: len(b.clients),
		EventTopics:      make(map[string]GuiEventTopicInfo),
		RpcTopics:        make(map[string]int),
		TopicConfigs:     b.config.TopicConfigs,
	}
	allTopics := make(map[string]bool)
	for topic := range b.eventSubs {
		allTopics[topic] = true
	}
	keys, _ := b.rdb.Keys(b.ctx, redisMessagesPrefix+"*").Result()
	for _, key := range keys {
		topic := strings.TrimPrefix(key, redisMessagesPrefix)
		allTopics[topic] = true
	}
	for topic := range allTopics {
		info := GuiEventTopicInfo{Groups: []GuiSubscriberGroup{}}
		if subs, ok := b.eventSubs[topic]; ok {
			for subID, clients := range subs {
				info.Groups = append(info.Groups, GuiSubscriberGroup{
					ID:      subID,
					Clients: len(clients),
				})
			}
		}
		count, err := b.rdb.LLen(b.ctx, redisMessagesPrefix+topic).Result()
		if err == nil {
			info.StoredMessageCount = count
		}
		state.EventTopics[topic] = info
	}
	for topic, handlers := range b.rpcHandlers {
		state.RpcTopics[topic] = len(handlers)
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		b.logger.Error("error marshaling GUI state", "error", err)
		return
	}
	b.hub.broadcast <- stateBytes
}

// --- Main Function ---

func loadConfigFromRedis(ctx context.Context, rdb *redis.Client, logger *slog.Logger) (Config, error) {
	configMap, err := rdb.HGetAll(ctx, redisConfigKey).Result()
	if err != nil {
		return Config{}, fmt.Errorf("could not load config from redis: %w", err)
	}

	cfg := Config{
		MaxRetries:     5,
		InitialBackoff: 250 * time.Millisecond,
		TopicConfigs:   make(map[string]TopicConfig),
		TLSEnabled:     false,
		CertFile:       "server.crt",
		KeyFile:        "server.key",
	}

	if val, ok := configMap["max_retries"]; ok {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.MaxRetries = intVal
		}
	}
	if val, ok := configMap["initial_backoff_ms"]; ok {
		if intVal, err := strconv.Atoi(val); err == nil {
			cfg.InitialBackoff = time.Duration(intVal) * time.Millisecond
		}
	}
	if val, ok := configMap["tls_enabled"]; ok {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			cfg.TLSEnabled = boolVal
		}
	}
	if val, ok := configMap["cert_file"]; ok {
		cfg.CertFile = val
	}
	if val, ok := configMap["key_file"]; ok {
		cfg.KeyFile = val
	}

	users, err := rdb.HGetAll(ctx, redisUsersKey).Result()
	if err != nil && err != redis.Nil {
		return Config{}, fmt.Errorf("could not load users from redis: %w", err)
	}
	if users == nil {
		users = make(map[string]string)
	}

	if _, ok := users["admin"]; !ok {
		logger.Info("no admin user found, creating default", "user", "admin", "password", "admin")
		users["admin"] = "admin"
		if err := rdb.HSet(ctx, redisUsersKey, "admin", "admin").Err(); err != nil {
			logger.Warn("failed to save default admin user to Redis", "error", err)
		}
	}
	cfg.Users = users

	topicConfigsJSON, err := rdb.Get(ctx, redisTopicConfigKey).Result()
	if err == nil {
		if err := json.Unmarshal([]byte(topicConfigsJSON), &cfg.TopicConfigs); err != nil {
			logger.Warn("could not parse topic configs from Redis", "error", err)
		}
	} else if err != redis.Nil {
		logger.Warn("could not fetch topic configs from Redis", "error", err)
	}

	logger.Info("configuration loaded from Redis", "config", fmt.Sprintf("%+v", cfg))
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		logger.Error("could not connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Redis", "address", redisAddr)

	config, err := loadConfigFromRedis(ctx, rdb, logger)
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	hub := newHub()
	go hub.run()

	broker, err := NewBroker(config, rdb, ctx, hub, logger)
	if err != nil {
		logger.Error("failed to initialize broker", "error", err)
		os.Exit(1)
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	httpServer := &http.Server{Addr: ":8080"}
	startHttpServer(broker, httpServer)

	go func() {
		if err := broker.Start(":8772"); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("TRPC broker failed", "error", err)
			signal.Stop(nil) // Trigger shutdown
		}
	}()

	go func() {
		logger.Info("HTTP server starting", "address", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
			signal.Stop(nil) // Trigger shutdown
		}
	}()

	<-shutdownChan
	logger.Info("shutdown signal received, starting graceful shutdown")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown failed", "error", err)
	}

	broker.Shutdown()
	logger.Info("graceful shutdown complete")
}

// --- HTTP Server for GUI ---
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func startHttpServer(b *Broker, server *http.Server) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var creds struct {
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		adminPass, ok := b.config.Users["admin"]
		if !ok || creds.Password != adminPass {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			b.logger.Error("websocket upgrade failed", "error", err)
			return
		}
		b.hub.register <- conn
		b.broadcastState()
	})
	mux.HandleFunc("/api/config/topic", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Topic  string      `json:"topic"`
			Config TopicConfig `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		b.mu.Lock()
		b.config.TopicConfigs[req.Topic] = req.Config
		configBytes, err := json.Marshal(b.config.TopicConfigs)
		b.mu.Unlock()
		if err != nil {
			http.Error(w, "Failed to marshal config", http.StatusInternalServerError)
			return
		}
		if err := b.rdb.Set(b.ctx, redisTopicConfigKey, configBytes, 0).Err(); err != nil {
			http.Error(w, "Failed to save config to Redis", http.StatusInternalServerError)
			return
		}
		b.logger.Info("updated topic config", "topic", req.Topic, "config", fmt.Sprintf("%+v", req.Config))
		b.broadcastState()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/config/admin/password", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			CurrentPassword string `json:"currentPassword"`
			NewPassword     string `json:"newPassword"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		adminPass, ok := b.config.Users["admin"]
		if !ok || req.CurrentPassword != adminPass {
			http.Error(w, "Unauthorized: Invalid current password", http.StatusUnauthorized)
			return
		}
		b.config.Users["admin"] = req.NewPassword
		if err := b.rdb.HSet(b.ctx, redisUsersKey, "admin", req.NewPassword).Err(); err != nil {
			http.Error(w, "Failed to save new password to Redis", http.StatusInternalServerError)
			b.config.Users["admin"] = adminPass
			return
		}
		b.logger.Info("admin password has been updated")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/api/topic/", func(w http.ResponseWriter, r *http.Request) {
		topic := strings.TrimPrefix(r.URL.Path, "/api/topic/")
		redisKey := redisMessagesPrefix + topic
		switch r.Method {
		case http.MethodGet:
			messages, err := b.rdb.LRange(b.ctx, redisKey, 0, 99).Result()
			if err != nil {
				http.Error(w, "Failed to get messages from Redis", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(messages)
		case http.MethodPost:
			var req struct {
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			b.mu.Lock()
			b.publishInternal(topic, req.Payload)
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if err := b.rdb.Del(b.ctx, redisKey).Err(); err != nil {
				http.Error(w, "Failed to purge messages from Redis", http.StatusInternalServerError)
				return
			}
			b.logger.Info("purged all messages for topic", "topic", topic)
			b.broadcastState()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server.Handler = mux
}
