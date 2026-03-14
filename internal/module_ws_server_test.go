package internal

import (
	"context"
	"testing"
)

func TestWSServerModule_InitStart(t *testing.T) {
	m, err := newWSServerModule("ws", map[string]any{
		"path":           "/ws",
		"maxConnections": float64(100),
		"pingInterval":   "30s",
		"maxMessageSize": float64(65536),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := m.Init(); err != nil {
		t.Fatalf("init error: %v", err)
	}

	if m.(*wsServerModule).hub == nil {
		t.Fatal("hub should be initialized after Init")
	}

	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("stop error: %v", err)
	}
}

func TestWSServerModule_DefaultConfig(t *testing.T) {
	m, err := newWSServerModule("ws", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mod := m.(*wsServerModule)
	if mod.path != "/ws" {
		t.Fatalf("expected default path /ws, got %s", mod.path)
	}
	if mod.maxConnections != 1000 {
		t.Fatalf("expected default maxConnections 1000, got %d", mod.maxConnections)
	}
}
