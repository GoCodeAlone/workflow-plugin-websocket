package internal

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func setupTestHub(t *testing.T) (*hub, func()) {
	t.Helper()
	h := newHub()
	go h.run()

	globalHubMu.Lock()
	prev := globalHub
	globalHub = h
	globalHubMu.Unlock()

	return h, func() {
		globalHubMu.Lock()
		globalHub = prev
		globalHubMu.Unlock()
	}
}

func waitForConns(t *testing.T, h *hub, ids ...string) {
	t.Helper()
	ready := make(chan struct{})
	go func() {
		for {
			h.mu.RLock()
			found := 0
			for _, id := range ids {
				if _, ok := h.connections[id]; ok {
					found++
				}
			}
			h.mu.RUnlock()
			if found == len(ids) {
				close(ready)
				return
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for connections %v to register", ids)
	}
}

func TestWSSendStep(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	conn := &connection{id: "conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	waitForConns(t, h, "conn-1")

	step, err := newWSSendStep("send", nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := step.Execute(context.Background(), nil, nil,
		map[string]any{"connectionId": "conn-1", "message": `{"type":"hello"}`},
		nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["sent"] != true {
		t.Fatal("expected sent=true")
	}

	msg := <-conn.send
	var parsed map[string]any
	json.Unmarshal(msg, &parsed)
	if parsed["type"] != "hello" {
		t.Fatalf("expected hello message, got %s", string(msg))
	}
}

func TestWSBroadcastStep(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	conn1 := &connection{id: "conn-1", send: make(chan []byte, 256)}
	conn2 := &connection{id: "conn-2", send: make(chan []byte, 256)}
	h.register <- conn1
	h.register <- conn2
	waitForConns(t, h, "conn-1", "conn-2")

	h.joinRoom("conn-1", "game-1")
	h.joinRoom("conn-2", "game-1")

	step, err := newWSBroadcastStep("bcast", nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := step.Execute(context.Background(), nil, nil,
		map[string]any{"room": "game-1", "message": `{"event":"start"}`},
		nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["recipients"].(int) != 2 {
		t.Fatalf("expected 2 recipients, got %v", result.Output["recipients"])
	}
}

func TestWSRoomJoinStep(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	conn := &connection{id: "conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	waitForConns(t, h, "conn-1")

	step, err := newWSRoomJoinStep("join", nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := step.Execute(context.Background(), nil, nil,
		map[string]any{"connectionId": "conn-1", "room": "lobby"},
		nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["joined"] != true {
		t.Fatal("expected joined=true")
	}

	members := h.roomMembers("lobby")
	if len(members) != 1 {
		t.Fatalf("expected 1 room member, got %d", len(members))
	}
}

func TestWSRoomListStep(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	conn1 := &connection{id: "conn-1", send: make(chan []byte, 256)}
	conn2 := &connection{id: "conn-2", send: make(chan []byte, 256)}
	h.register <- conn1
	h.register <- conn2
	waitForConns(t, h, "conn-1", "conn-2")

	h.joinRoom("conn-1", "room-a")
	h.joinRoom("conn-2", "room-a")

	step, err := newWSRoomListStep("list", nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := step.Execute(context.Background(), nil, nil,
		map[string]any{"room": "room-a"},
		nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	connections := result.Output["connections"].([]string)
	if len(connections) != 2 {
		t.Fatalf("expected 2 connections, got %d", len(connections))
	}
}
