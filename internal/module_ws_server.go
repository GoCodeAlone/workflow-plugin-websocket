package internal

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

var (
	globalHub   *hub
	globalHubMu sync.RWMutex
)

// GetHub returns the global WebSocket hub for step types to use.
func GetHub() *hub {
	globalHubMu.RLock()
	defer globalHubMu.RUnlock()
	return globalHub
}

type wsServerModule struct {
	name           string
	path           string
	maxConnections int
	pingInterval   time.Duration
	pongWait       time.Duration
	maxMessageSize int64
	authRequired   bool
	hub            *hub
	upgrader       websocket.Upgrader
	onMessage      func(connID string, msgType int, msg []byte)
}

func newWSServerModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	m := &wsServerModule{
		name:           name,
		path:           "/ws",
		maxConnections: 1000,
		pingInterval:   30 * time.Second,
		maxMessageSize: 64 * 1024,
	}

	if v, ok := config["path"].(string); ok && v != "" {
		m.path = v
	}
	if v, ok := config["maxConnections"].(float64); ok {
		m.maxConnections = int(v)
	}
	if v, ok := config["pingInterval"].(string); ok {
		d, err := time.ParseDuration(v)
		if err == nil {
			m.pingInterval = d
		}
	}
	if v, ok := config["pongWait"].(string); ok {
		d, err := time.ParseDuration(v)
		if err == nil {
			m.pongWait = d
		}
	}
	if v, ok := config["maxMessageSize"].(float64); ok {
		m.maxMessageSize = int64(v)
	}
	if v, ok := config["authRequired"].(bool); ok {
		m.authRequired = v
	}

	m.upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	return m, nil
}

func (m *wsServerModule) Init() error {
	m.hub = newHub()
	m.hub.maxMessageSize = m.maxMessageSize
	m.hub.pingPeriod = m.pingInterval
	if m.pongWait > 0 {
		m.hub.pongWait = m.pongWait
	} else {
		m.hub.pongWait = m.pingInterval * 2
	}

	globalHubMu.Lock()
	globalHub = m.hub
	globalHubMu.Unlock()

	go m.hub.run()
	return nil
}

func (m *wsServerModule) Start(ctx context.Context) error {
	return nil
}

func (m *wsServerModule) Stop(ctx context.Context) error {
	globalHubMu.Lock()
	if globalHub == m.hub {
		globalHub = nil
	}
	globalHubMu.Unlock()

	// Close all active connections gracefully (triggers WebSocket close frames via writePump).
	m.hub.mu.RLock()
	conns := make([]*connection, 0, len(m.hub.connections))
	for _, c := range m.hub.connections {
		conns = append(conns, c)
	}
	m.hub.mu.RUnlock()
	for _, c := range conns {
		c.close()
	}

	m.hub.stop()
	return nil
}

// HTTPPath returns the path this module wants to handle.
func (m *wsServerModule) HTTPPath() string { return m.path }

// ServeHTTP handles WebSocket upgrade requests.
// The host workflow engine routes HTTP requests to this handler.
func (m *wsServerModule) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.hub.mu.RLock()
	count := len(m.hub.connections)
	m.hub.mu.RUnlock()

	if count >= m.maxConnections {
		http.Error(w, "max connections reached", http.StatusServiceUnavailable)
		return
	}

	wsConn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	connID := uuid.New().String()
	conn := &connection{
		id:   connID,
		conn: wsConn,
		send: make(chan wsMessage, 256),
		hub:  m.hub,
	}

	// registerSync adds the connection to connRooms under the mutex before returning,
	// so callGlobalWSConnectHandler (which fires ws_connect → step.ws_room_join → joinRoom)
	// always finds the connection registered. Using the channel (m.hub.register <- conn)
	// caused a race: the hub run-loop may not have processed the channel item yet when
	// the connect handler fires.
	m.hub.registerSync(conn)

	// Extract URL query params so ws_connect pipelines can reference them in templates
	// (e.g. {{ .sessionID }} from ?sessionId=<id>). We copy all params and also add
	// the canonical sessionID alias so both {{ .sessionId }} and {{ .sessionID }} work.
	extra := make(map[string]any, len(r.URL.Query()))
	for k, vals := range r.URL.Query() {
		if len(vals) > 0 {
			extra[k] = vals[0]
		}
	}
	if sid, ok := extra["sessionId"]; ok {
		extra["sessionID"] = sid
	}

	go callGlobalWSConnectHandler(connID, extra)
	go conn.writePump()
	go conn.readPump(func(connID string, msgType int, msg []byte) {
		callGlobalWSMessageHandler(connID, msgType, msg)
		if m.onMessage != nil {
			m.onMessage(connID, msgType, msg)
		}
	})
}

// SetMessageHandler allows the trigger system to receive incoming WS messages.
func (m *wsServerModule) SetMessageHandler(handler func(connID string, msgType int, msg []byte)) {
	m.onMessage = handler
}
