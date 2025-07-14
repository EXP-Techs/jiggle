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
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

const (
	// File names for persistence
	walFile = "trpc_broker.wal"

	// Redis keys
	redisConfigKey      = "trpc:config"
	redisUsersKey       = "trpc:users"
	redisTopicConfigKey = "trpc:config:topics"
	redisMessagesPrefix = "trpc:messages:"
)

// --- GUI Hub for WebSockets ---
type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func newHub() *Hub { /* ... same as before ... */
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		clients:    make(map[*websocket.Conn]bool),
	}
}
func (h *Hub) run() { /* ... same as before ... */
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
					log.Printf("Error broadcasting to websocket: %v", err)
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
}

type Message struct {
	Topic         string          `json:"topic"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlation_id,omitempty"`
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

	eventSubs       map[string]map[string][]*Client
	eventSubIndex   map[string]map[string]int
	clientSubs      map[string][]Subscription
	rpcHandlers     map[string][]*Client
	rpcHandlerIndex map[string]int
	pendingReqs     map[string]*Client
	walFile         *os.File
}

func NewBroker(config Config, rdb *redis.Client, ctx context.Context, hub *Hub) (*Broker, error) {
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
		eventSubs:       make(map[string]map[string][]*Client),
		eventSubIndex:   make(map[string]map[string]int),
		clientSubs:      make(map[string][]Subscription),
		rpcHandlers:     make(map[string][]*Client),
		rpcHandlerIndex: make(map[string]int),
		pendingReqs:     make(map[string]*Client),
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
		log.Printf("TRPC Broker listening securely with TLS on %s", addr)
	} else {
		l, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		log.Printf("TRPC Broker listening (insecure) on %s", addr)
	}

	b.listener = l

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) Close() { /* ... same as before ... */
	b.listener.Close()
	b.walFile.Close()
}
func (b *Broker) handleConnection(conn net.Conn) { /* ... same as before ... */
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := b.authenticate(conn, reader); err != nil {
		log.Printf("Authentication failed for %s: %v", conn.RemoteAddr().String(), err)
		return
	}
	log.Printf("Client authenticated: %s", conn.RemoteAddr().String())
	clientID := conn.RemoteAddr().String()
	client := &Client{conn: conn, id: clientID}
	b.mu.Lock()
	b.clients[clientID] = client
	b.mu.Unlock()
	b.broadcastState()
	log.Printf("Client connected: %s", clientID)
	defer func() {
		b.cleanupClient(client)
		log.Printf("Client disconnected: %s", clientID)
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client %s: %v", clientID, err)
			}
			return
		}
		b.processCommand(client, line)
	}
}
func (b *Broker) authenticate(conn net.Conn, reader *bufio.Reader) error { /* ... same as before ... */
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
func (b *Broker) cleanupClient(client *Client) { /* ... same as before ... */
	b.mu.Lock()
	delete(b.clients, client.id)
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
func (b *Broker) getTopicConfig(topic string) TopicConfig { /* ... same as before ... */
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
func (b *Broker) sendMessageWithRetry(client *Client, msg Message) { /* ... same as before ... */
	topicCfg := b.getTopicConfig(msg.Topic)
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("FATAL: could not marshal message for retry: %v", err)
		return
	}
	msgBytes = append(msgBytes, '\n')
	backoff := time.Duration(topicCfg.InitialBackoff) * time.Millisecond
	for i := 0; i < topicCfg.MaxRetries; i++ {
		_, err := client.conn.Write(msgBytes)
		if err == nil {
			return
		}
		log.Printf("Attempt %d: failed to send message to %s: %v. Retrying in %v", i+1, client.id, err, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	log.Printf("All retries failed for client %s. Republishing message to DLQ.", client.id)
	b.publishToDLQ(msg, topicCfg)
}
func (b *Broker) publishToDLQ(msg Message, cfg TopicConfig) { /* ... same as before ... */
	dlqTopic := cfg.DLQTopic
	log.Printf("Republishing failed message to DLQ topic: %s", dlqTopic)
	b.mu.Lock()
	b.publishInternal(dlqTopic, msg.Payload)
	b.mu.Unlock()
}
func (b *Broker) processCommand(client *Client, rawCmd []byte) { /* ... same as before ... */
	b.mu.Lock()
	if _, err := b.walFile.Write(rawCmd); err != nil {
		log.Printf("CRITICAL: Failed to write to WAL: %v", err)
	}
	b.mu.Unlock()
	var cmd Command
	if err := json.Unmarshal(rawCmd, &cmd); err != nil {
		log.Printf("Failed to unmarshal command from %s: %v", client.id, err)
		return
	}
	log.Printf("Received command: %+v from %s", cmd, client.id)
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
	default:
		log.Printf("Unknown command action: %s", cmd.Action)
	}
}
func (b *Broker) publishInternal(topic string, payload json.RawMessage) { /* ... same as before ... */
	subscriberGroups, ok := b.eventSubs[topic]
	if !ok || len(subscriberGroups) == 0 {
		redisKey := redisMessagesPrefix + topic
		msgToStore, _ := json.Marshal(Message{Topic: topic, Payload: payload})
		if err := b.rdb.LPush(b.ctx, redisKey, msgToStore).Err(); err != nil {
			log.Printf("CRITICAL: Failed to store message for topic '%s' in Redis: %v", topic, err)
		} else {
			log.Printf("No subscribers for topic '%s'. Stored message in Redis.", topic)
		}
		go b.broadcastState()
		return
	}
	msg := Message{Topic: topic, Payload: payload}
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
		log.Printf("Load-balancing event for topic '%s' to subscriber group '%s' (client: %s)", topic, subID, selectedClient.id)
		go b.sendMessageWithRetry(selectedClient, msg)
	}
}
func (b *Broker) handleSubscribe(client *Client, cmd Command) { /* ... same as before ... */
	b.mu.Lock()
	var payload struct {
		SubscriberID string `json:"subscriber_id"`
	}
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.SubscriberID == "" {
		log.Printf("Invalid SUBSCRIBE command from %s: missing or empty subscriber_id in payload", client.id)
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
	log.Printf("Client %s subscribed to event topic '%s' with ID '%s'", client.id, cmd.Topic, payload.SubscriberID)
	b.mu.Unlock()
	b.broadcastState()
}
func (b *Broker) handleRegister(client *Client, cmd Command) { /* ... same as before ... */
	b.mu.Lock()
	b.rpcHandlers[cmd.Topic] = append(b.rpcHandlers[cmd.Topic], client)
	log.Printf("Client %s registered as RPC handler for topic '%s'. Total handlers: %d", client.id, cmd.Topic, len(b.rpcHandlers[cmd.Topic]))
	b.mu.Unlock()
	b.broadcastState()
}
func (b *Broker) handlePublish(client *Client, cmd Command) { /* ... same as before ... */
	b.mu.Lock()
	b.publishInternal(cmd.Topic, cmd.Payload)
	b.mu.Unlock()
}
func (b *Broker) handleRequest(client *Client, cmd Command) { /* ... same as before ... */
	b.mu.Lock()
	handlers, ok := b.rpcHandlers[cmd.Topic]
	if !ok || len(handlers) == 0 {
		log.Printf("No handler for RPC topic '%s'", cmd.Topic)
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
	go b.sendMessageWithRetry(handler, msg)
	log.Printf("Forwarded REQUEST on topic '%s' to handler %s (round-robin)", cmd.Topic, handler.id)
}
func (b *Broker) handleRespond(client *Client, cmd Command) { /* ... same as before ... */
	b.mu.Lock()
	originalClient, ok := b.pendingReqs[cmd.CorrelationID]
	if !ok {
		log.Printf("Received RESPONSE for unknown correlation ID: %s", cmd.CorrelationID)
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
	go b.sendMessageWithRetry(originalClient, msg)
	log.Printf("Forwarded RESPONSE with correlation ID '%s' to client %s", cmd.CorrelationID, originalClient.id)
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

func (b *Broker) broadcastState() { /* ... same as before ... */
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
		log.Printf("Error marshaling GUI state: %v", err)
		return
	}
	b.hub.broadcast <- stateBytes
}

// --- Main Function ---

func loadConfigFromRedis(ctx context.Context, rdb *redis.Client) (Config, error) {
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
		log.Println("No admin user found. Creating default admin with password 'admin'.")
		users["admin"] = "admin"
		if err := rdb.HSet(ctx, redisUsersKey, "admin", "admin").Err(); err != nil {
			log.Printf("Warning: failed to save default admin user to Redis: %v", err)
		}
	}
	cfg.Users = users

	topicConfigsJSON, err := rdb.Get(ctx, redisTopicConfigKey).Result()
	if err == nil {
		if err := json.Unmarshal([]byte(topicConfigsJSON), &cfg.TopicConfigs); err != nil {
			log.Printf("Warning: could not parse topic configs from Redis: %v", err)
		}
	} else if err != redis.Nil {
		log.Printf("Warning: could not fetch topic configs from Redis: %v", err)
	}

	log.Printf("Configuration loaded from Redis: %+v", cfg)
	return cfg, nil
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx := context.Background()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	log.Printf("Connected to Redis at %s", redisAddr)

	config, err := loadConfigFromRedis(ctx, rdb)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	hub := newHub()
	go hub.run()

	broker, err := NewBroker(config, rdb, ctx, hub)
	if err != nil {
		log.Fatalf("Failed to initialize broker: %v", err)
	}
	defer broker.Close()

	go func() {
		if err := broker.Start(":8772"); err != nil {
			log.Fatalf("Failed to start TRPC broker: %v", err)
		}
	}()

	startHttpServer(broker)
}

// --- HTTP Server for GUI ---
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func startHttpServer(b *Broker) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
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
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}
		b.hub.register <- conn
		b.broadcastState()
	})
	http.HandleFunc("/api/config/topic", func(w http.ResponseWriter, r *http.Request) {
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
		log.Printf("Updated topic config for '%s': %+v", req.Topic, req.Config)
		b.broadcastState()
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/api/config/admin/password", func(w http.ResponseWriter, r *http.Request) {
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
		log.Println("Admin password has been updated.")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// New API endpoints for topic management
	http.HandleFunc("/api/topic/", func(w http.ResponseWriter, r *http.Request) {
		topic := strings.TrimPrefix(r.URL.Path, "/api/topic/")
		redisKey := redisMessagesPrefix + topic
		switch r.Method {
		case http.MethodGet: // Get messages
			messages, err := b.rdb.LRange(b.ctx, redisKey, 0, 99).Result()
			if err != nil {
				http.Error(w, "Failed to get messages from Redis", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(messages)
		case http.MethodPost: // Publish message
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
		case http.MethodDelete: // Purge messages
			if err := b.rdb.Del(b.ctx, redisKey).Err(); err != nil {
				http.Error(w, "Failed to purge messages from Redis", http.StatusInternalServerError)
				return
			}
			log.Printf("Purged all messages for topic '%s'", topic)
			b.broadcastState()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Println("HTTP server for GUI starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
