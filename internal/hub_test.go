package internal

import (
	"testing"
	"time"
)

func TestHub_RegisterUnregister(t *testing.T) {
	h := newHub()
	go h.run()
	t.Cleanup(func() { h.stop() })

	mockConn := &connection{id: "conn-1", send: make(chan []byte, 256)}
	h.register <- mockConn

	// Wait for registration
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, exists := h.connections[mockConn.id]
		h.mu.RUnlock()
		if exists {
			break
		}
		time.Sleep(time.Millisecond)
	}

	h.mu.RLock()
	_, exists := h.connections[mockConn.id]
	h.mu.RUnlock()
	if !exists {
		t.Fatal("connection should be registered")
	}

	h.unregister <- mockConn
	// Wait for unregistration
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, exists = h.connections[mockConn.id]
		h.mu.RUnlock()
		if !exists {
			break
		}
		time.Sleep(time.Millisecond)
	}

	h.mu.RLock()
	_, exists = h.connections[mockConn.id]
	h.mu.RUnlock()
	if exists {
		t.Fatal("connection should be unregistered")
	}
}

func TestHub_RoomJoinLeave(t *testing.T) {
	h := newHub()
	go h.run()
	t.Cleanup(func() { h.stop() })

	mockConn := &connection{id: "conn-1", send: make(chan []byte, 256)}
	h.register <- mockConn

	// Wait for registration
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, ok := h.connections["conn-1"]
		h.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(time.Millisecond)
	}

	h.joinRoom("conn-1", "game-room-1")

	members := h.roomMembers("game-room-1")
	if len(members) != 1 || members[0] != "conn-1" {
		t.Fatalf("expected [conn-1], got %v", members)
	}

	h.leaveRoom("conn-1", "game-room-1")

	members = h.roomMembers("game-room-1")
	if len(members) != 0 {
		t.Fatalf("expected empty room, got %v", members)
	}
}

func TestHub_Broadcast(t *testing.T) {
	h := newHub()
	go h.run()
	t.Cleanup(func() { h.stop() })

	conn1 := &connection{id: "conn-1", send: make(chan []byte, 256)}
	conn2 := &connection{id: "conn-2", send: make(chan []byte, 256)}
	h.register <- conn1
	h.register <- conn2

	// Wait for both to register
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, ok1 := h.connections["conn-1"]
		_, ok2 := h.connections["conn-2"]
		h.mu.RUnlock()
		if ok1 && ok2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	h.joinRoom("conn-1", "room-a")
	h.joinRoom("conn-2", "room-a")

	h.broadcastToRoom("room-a", []byte(`{"event":"test"}`), "")

	msg1 := <-conn1.send
	msg2 := <-conn2.send
	if string(msg1) != `{"event":"test"}` || string(msg2) != `{"event":"test"}` {
		t.Fatalf("both connections should receive broadcast")
	}
}

func TestHub_BroadcastExclude(t *testing.T) {
	h := newHub()
	go h.run()
	t.Cleanup(func() { h.stop() })

	conn1 := &connection{id: "conn-1", send: make(chan []byte, 256)}
	conn2 := &connection{id: "conn-2", send: make(chan []byte, 256)}
	h.register <- conn1
	h.register <- conn2

	// Wait for both to register
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, ok1 := h.connections["conn-1"]
		_, ok2 := h.connections["conn-2"]
		h.mu.RUnlock()
		if ok1 && ok2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	h.joinRoom("conn-1", "room-a")
	h.joinRoom("conn-2", "room-a")

	h.broadcastToRoom("room-a", []byte(`{"event":"test"}`), "conn-1")

	// Only conn2 should receive
	select {
	case msg := <-conn2.send:
		if string(msg) != `{"event":"test"}` {
			t.Fatalf("conn2 should receive broadcast")
		}
	default:
		t.Fatal("conn2 should have a message")
	}

	select {
	case <-conn1.send:
		t.Fatal("conn1 should be excluded from broadcast")
	default:
		// expected
	}
}
