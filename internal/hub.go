package internal

import (
	"sync"
	"time"
)

type hub struct {
	connections    map[string]*connection
	rooms          map[string]map[string]bool // room -> set of connIDs
	connRooms      map[string]map[string]bool // connID -> set of rooms
	register       chan *connection
	unregister     chan *connection
	done           chan struct{}
	mu             sync.RWMutex
	maxMessageSize int64
	pingPeriod     time.Duration
	pongWait       time.Duration
}

func newHub() *hub {
	return &hub{
		connections:    make(map[string]*connection),
		rooms:          make(map[string]map[string]bool),
		connRooms:      make(map[string]map[string]bool),
		register:       make(chan *connection),
		unregister:     make(chan *connection),
		done:           make(chan struct{}),
		maxMessageSize: 64 * 1024, // 64KB default
		pingPeriod:     30 * time.Second,
		pongWait:       60 * time.Second,
	}
}

func (h *hub) run() {
	for {
		select {
		case <-h.done:
			return

		case conn := <-h.register:
			h.mu.Lock()
			h.connections[conn.id] = conn
			h.connRooms[conn.id] = make(map[string]bool)
			h.mu.Unlock()

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.connections[conn.id]; ok {
				// Remove from all rooms
				for room := range h.connRooms[conn.id] {
					delete(h.rooms[room], conn.id)
					if len(h.rooms[room]) == 0 {
						delete(h.rooms, room)
					}
				}
				delete(h.connRooms, conn.id)
				delete(h.connections, conn.id)
			}
			h.mu.Unlock()
		}
	}
}

// stop signals the hub's run loop to exit.
func (h *hub) stop() {
	close(h.done)
}

func (h *hub) joinRoom(connID, room string) bool {
	if connID == "" || room == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// Only join if the connection is actually registered — prevents ghost members.
	if _, ok := h.connRooms[connID]; !ok {
		return false
	}
	if _, ok := h.rooms[room]; !ok {
		h.rooms[room] = make(map[string]bool)
	}
	h.rooms[room][connID] = true
	h.connRooms[connID][room] = true
	return true
}

func (h *hub) leaveRoom(connID, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if members, ok := h.rooms[room]; ok {
		delete(members, connID)
		if len(members) == 0 {
			delete(h.rooms, room)
		}
	}
	if rooms, ok := h.connRooms[connID]; ok {
		delete(rooms, room)
	}
}

func (h *hub) roomMembers(room string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	members, ok := h.rooms[room]
	if !ok {
		return nil
	}
	result := make([]string, 0, len(members))
	for id := range members {
		result = append(result, id)
	}
	return result
}

// sendTo delivers msg to a connection. Returns false if the connection is gone
// or the send buffer is full. Uses recover to guard against send-on-closed-channel
// when a connection closes concurrently with a send attempt.
func (h *hub) sendTo(connID string, msg []byte) (sent bool) {
	h.mu.RLock()
	conn, ok := h.connections[connID]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	select {
	case conn.send <- msg:
		return true
	default:
		return false
	}
}

func (h *hub) broadcastToRoom(room string, msg []byte, exclude string) {
	h.mu.RLock()
	members, ok := h.rooms[room]
	if !ok {
		h.mu.RUnlock()
		return
	}
	// Copy member list under read lock
	ids := make([]string, 0, len(members))
	for id := range members {
		if id != exclude {
			ids = append(ids, id)
		}
	}
	h.mu.RUnlock()

	for _, id := range ids {
		h.sendTo(id, msg)
	}
}

func (h *hub) broadcastAll(msg []byte, exclude string) {
	h.mu.RLock()
	ids := make([]string, 0, len(h.connections))
	for id := range h.connections {
		if id != exclude {
			ids = append(ids, id)
		}
	}
	h.mu.RUnlock()

	for _, id := range ids {
		h.sendTo(id, msg)
	}
}

// closeConnection closes a connection by draining its send channel, which
// causes writePump to send a WebSocket close frame and exit cleanly.
// Returns false if the connection does not exist.
func (h *hub) closeConnection(connID string) bool {
	h.mu.RLock()
	conn, ok := h.connections[connID]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	conn.close()
	return true
}
