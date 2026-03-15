package internal

import (
	"testing"
	"time"
)

func TestSpectatorRoom_JoinWithMode(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spectator-1", send: make(chan []byte, 256)}
	h.register <- conn
	// Small sleep to let the register be processed
	time.Sleep(10 * time.Millisecond)

	h.spectatorJoin("spectator-1", "game-123", SpectatorMeta{
		Mode:                "anonymous",
		PerspectivePlayerID: "",
	})

	meta, ok := h.getSpectatorMeta("spectator-1", "game-123")
	if !ok {
		t.Fatal("spectator meta should be stored")
	}
	if meta.Mode != "anonymous" {
		t.Fatalf("expected mode=anonymous, got %q", meta.Mode)
	}
	if h.spectatorCount("game-123") != 1 {
		t.Fatalf("expected spectator count 1, got %d", h.spectatorCount("game-123"))
	}
}

func TestSpectatorRoom_ModeSwitch(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spectator-1", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	h.spectatorJoin("spectator-1", "game-123", SpectatorMeta{Mode: "anonymous"})
	h.spectatorSetMode("spectator-1", "game-123", SpectatorMeta{
		Mode:                "player",
		PerspectivePlayerID: "player-0",
	})

	meta, _ := h.getSpectatorMeta("spectator-1", "game-123")
	if meta.Mode != "player" {
		t.Fatalf("expected mode=player after switch, got %q", meta.Mode)
	}
	if meta.PerspectivePlayerID != "player-0" {
		t.Fatalf("expected perspectivePlayerId=player-0, got %q", meta.PerspectivePlayerID)
	}
}

func TestSpectatorRoom_LeaveDecrementsCount(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	for _, id := range []string{"s-1", "s-2"} {
		conn := &connection{id: id, send: make(chan []byte, 256)}
		h.register <- conn
	}
	time.Sleep(10 * time.Millisecond)

	for _, id := range []string{"s-1", "s-2"} {
		h.spectatorJoin(id, "game-123", SpectatorMeta{Mode: "anonymous"})
	}

	if h.spectatorCount("game-123") != 2 {
		t.Fatalf("expected 2 spectators, got %d", h.spectatorCount("game-123"))
	}

	h.spectatorLeave("s-1", "game-123")
	if h.spectatorCount("game-123") != 1 {
		t.Fatalf("expected 1 spectator after leave, got %d", h.spectatorCount("game-123"))
	}
}

func TestSpectatorRoom_JoinAddsToWSRoom(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	h.spectatorJoin("spec-conn", "game-abc", SpectatorMeta{Mode: "omniscient"})

	// The connection should be in the spectator:game-abc WebSocket room
	members := h.roomMembers("spectator:game-abc")
	found := false
	for _, m := range members {
		if m == "spec-conn" {
			found = true
		}
	}
	if !found {
		t.Fatal("spectatorJoin must add connection to spectator:<gameId> room")
	}
}

func TestSpectatorRoom_LeaveRemovesFromWSRoom(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	h.spectatorJoin("spec-conn", "game-abc", SpectatorMeta{Mode: "anonymous"})
	h.spectatorLeave("spec-conn", "game-abc")

	members := h.roomMembers("spectator:game-abc")
	for _, m := range members {
		if m == "spec-conn" {
			t.Fatal("spectatorLeave must remove connection from spectator:<gameId> room")
		}
	}
}
