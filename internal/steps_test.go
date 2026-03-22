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
		h.stop()
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
			time.Sleep(time.Millisecond)
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

	conn := &connection{id: "conn-1", send: make(chan wsMessage, 256)}
	h.register <- conn
	waitForConns(t, h, "conn-1")

	step, err := newWSSendStep("send", nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := step.Execute(context.Background(), nil, nil,
		nil, nil,
		map[string]any{"connectionId": "conn-1", "message": `{"type":"hello"}`})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["sent"] != true {
		t.Fatal("expected sent=true")
	}

	msg := <-conn.send
	var parsed map[string]any
	json.Unmarshal(msg.data, &parsed)
	if parsed["type"] != "hello" {
		t.Fatalf("expected hello message, got %s", string(msg.data))
	}
}
