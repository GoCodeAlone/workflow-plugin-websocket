package internal

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestWSTrigger_ReceivesMessage(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	// Register a connection in a room.
	conn := &connection{id: "conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	// Wait for registration.
	for {
		h.mu.RLock()
		_, ok := h.connections["conn-1"]
		h.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	h.joinRoom("conn-1", "game-room-1")

	// Set up trigger with a callback that captures the received data.
	var (
		mu          sync.Mutex
		gotAction   string
		gotData     map[string]any
		callbackDone = make(chan struct{})
	)

	cb := func(action string, data map[string]any) error {
		mu.Lock()
		gotAction = action
		gotData = data
		mu.Unlock()
		close(callbackDone)
		return nil
	}

	p := &wsPlugin{}
	trigger, err := p.CreateTrigger("websocket", nil, cb)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	if err := trigger.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer trigger.Stop(context.Background()) //nolint:errcheck

	// Simulate an inbound WS message.
	msg := `{"type":"play_card","card":"ace-of-spades"}`
	callGlobalWSMessageHandler("conn-1", []byte(msg))

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback not called within 1s")
	}

	mu.Lock()
	defer mu.Unlock()

	if gotAction != "message" {
		t.Errorf("expected action %q, got %q", "message", gotAction)
	}
	if gotData["connectionId"] != "conn-1" {
		t.Errorf("expected connectionId %q, got %v", "conn-1", gotData["connectionId"])
	}
	if gotData["message"] != msg {
		t.Errorf("expected message %q, got %v", msg, gotData["message"])
	}
	if gotData["room"] != "game-room-1" {
		t.Errorf("expected room %q, got %v", "game-room-1", gotData["room"])
	}

	// Verify JSON payload was decoded.
	payload, ok := gotData["payload"].(map[string]any)
	if !ok {
		t.Fatalf("expected payload to be map[string]any, got %T", gotData["payload"])
	}
	if payload["type"] != "play_card" {
		t.Errorf("expected payload type %q, got %v", "play_card", payload["type"])
	}
}

func TestWSTrigger_NoRoom(t *testing.T) {
	h, cleanup := setupTestHub(t)
	defer cleanup()

	conn := &connection{id: "conn-2", send: make(chan []byte, 256)}
	h.register <- conn
	for {
		h.mu.RLock()
		_, ok := h.connections["conn-2"]
		h.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(time.Millisecond)
	}

	var (
		mu           sync.Mutex
		gotData      map[string]any
		callbackDone = make(chan struct{})
	)

	cb := func(_ string, data map[string]any) error {
		mu.Lock()
		gotData = data
		mu.Unlock()
		close(callbackDone)
		return nil
	}

	p := &wsPlugin{}
	trigger, err := p.CreateTrigger("websocket", nil, cb)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if err := trigger.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer trigger.Stop(context.Background()) //nolint:errcheck

	callGlobalWSMessageHandler("conn-2", []byte(`{"ping":true}`))

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback not called within 1s")
	}

	mu.Lock()
	defer mu.Unlock()

	if gotData["room"] != "" {
		t.Errorf("expected empty room, got %v", gotData["room"])
	}
}

func TestWSTrigger_StopClearsHandler(t *testing.T) {
	_, cleanup := setupTestHub(t)
	defer cleanup()

	cb := func(_ string, _ map[string]any) error { return nil }

	p := &wsPlugin{}
	trigger, err := p.CreateTrigger("websocket", nil, cb)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if err := trigger.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify handler is installed.
	globalWSMessageHandlerMu.RLock()
	if globalWSMessageHandler == nil {
		t.Error("expected global handler to be set after Start")
	}
	globalWSMessageHandlerMu.RUnlock()

	if err := trigger.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Verify handler is cleared.
	globalWSMessageHandlerMu.RLock()
	if globalWSMessageHandler != nil {
		t.Error("expected global handler to be nil after Stop")
	}
	globalWSMessageHandlerMu.RUnlock()
}

func TestWSTrigger_NonJSONMessage(t *testing.T) {
	_, cleanup := setupTestHub(t)
	defer cleanup()

	var (
		mu           sync.Mutex
		gotData      map[string]any
		callbackDone = make(chan struct{})
	)

	cb := func(_ string, data map[string]any) error {
		mu.Lock()
		gotData = data
		mu.Unlock()
		close(callbackDone)
		return nil
	}

	p := &wsPlugin{}
	trigger, err := p.CreateTrigger("websocket", nil, cb)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if err := trigger.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer trigger.Stop(context.Background()) //nolint:errcheck

	callGlobalWSMessageHandler("conn-x", []byte("plain text message"))

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback not called within 1s")
	}

	mu.Lock()
	defer mu.Unlock()

	if gotData["message"] != "plain text message" {
		t.Errorf("expected message %q, got %v", "plain text message", gotData["message"])
	}
	// Non-JSON: payload field should not be present or should be nil.
	if _, hasPayload := gotData["payload"]; hasPayload {
		_, isMap := gotData["payload"].(map[string]any)
		if !isMap {
			t.Errorf("payload should be absent for non-JSON messages, got %T", gotData["payload"])
		}
	}
}

// TestWSTrigger_CreateTriggerPlugin verifies plugin-level TriggerTypes/CreateTrigger.
func TestWSTrigger_CreateTriggerPlugin(t *testing.T) {
	p := &wsPlugin{}
	types := p.TriggerTypes()
	if len(types) != 1 || types[0] != "websocket" {
		t.Fatalf("expected [websocket], got %v", types)
	}

	cb := func(_ string, _ map[string]any) error { return nil }
	trigger, err := p.CreateTrigger("websocket", nil, cb)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if trigger == nil {
		t.Fatal("expected non-nil trigger")
	}

	_, err = p.CreateTrigger("unknown", nil, cb)
	if err == nil {
		t.Fatal("expected error for unknown trigger type")
	}
}

// Compile-time checks.
var (
	_ interface{ Init() error } = (*wsTrigger)(nil)
	_ = json.Marshal            // keep json import
)
