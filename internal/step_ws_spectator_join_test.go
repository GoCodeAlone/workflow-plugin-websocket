package internal

import (
	"context"
	"testing"
	"time"
)

func TestWSSpectatorJoinStep_Execute(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	// Set the global hub so steps can access it
	globalHub = h

	step := &wsSpectatorJoinStep{
		connID: "spec-conn-1",
		gameID: "game-abc",
		mode:   "anonymous",
	}

	out, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Output["room"] != "spectator:game-abc" {
		t.Fatalf("unexpected room: %v", out.Output["room"])
	}
	if out.Output["spectatorCount"].(int) != 1 {
		t.Fatalf("expected spectatorCount=1, got %v", out.Output["spectatorCount"])
	}
}

func TestWSSpectatorModeSwitchStep_Execute(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	globalHub = h
	h.spectatorJoin("spec-conn-1", "game-abc", SpectatorMeta{Mode: "anonymous"})

	step := &wsSpectatorModeSwitchStep{
		connID:              "spec-conn-1",
		gameID:              "game-abc",
		newMode:             "player",
		perspectivePlayerID: "player-0",
	}

	_, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	meta, ok := h.getSpectatorMeta("spec-conn-1", "game-abc")
	if !ok || meta.Mode != "player" || meta.PerspectivePlayerID != "player-0" {
		t.Fatalf("mode switch not applied: %+v", meta)
	}
}

func TestWSSpectatorLeaveStep_Execute(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	globalHub = h
	h.spectatorJoin("spec-conn-1", "game-abc", SpectatorMeta{Mode: "anonymous"})

	if h.spectatorCount("game-abc") != 1 {
		t.Fatalf("expected 1 spectator before leave")
	}

	step := &wsSpectatorLeaveStep{
		connID: "spec-conn-1",
		gameID: "game-abc",
	}

	out, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.spectatorCount("game-abc") != 0 {
		t.Fatalf("expected 0 spectators after leave")
	}
	if out.Output["spectatorCount"].(int) != 0 {
		t.Fatalf("expected spectatorCount=0 in output, got %v", out.Output["spectatorCount"])
	}
}

func TestWSSpectatorModeSwitchStep_PlayerModeRequiresPerspectiveId(t *testing.T) {
	h := newHub()
	go h.run()
	defer h.stop()

	conn := &connection{id: "spec-conn-1", send: make(chan []byte, 256)}
	h.register <- conn
	time.Sleep(10 * time.Millisecond)

	globalHub = h
	h.spectatorJoin("spec-conn-1", "game-abc", SpectatorMeta{Mode: "anonymous"})

	step := &wsSpectatorModeSwitchStep{
		connID:  "spec-conn-1",
		gameID:  "game-abc",
		newMode: "player",
		// perspectivePlayerID intentionally empty
	}

	_, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error when perspectivePlayerId is missing for player mode")
	}
}
