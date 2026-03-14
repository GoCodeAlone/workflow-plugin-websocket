package internal

import (
	"context"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// --- Room Join ---

type wsRoomJoinStep struct{ name string }

func newWSRoomJoinStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsRoomJoinStep{name: name}, nil
}

func (s *wsRoomJoinStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "joined": false}}, nil
	}

	connID, _ := current["connectionId"].(string)
	room, _ := current["room"].(string)

	h.joinRoom(connID, room)
	return &sdk.StepResult{Output: map[string]any{"joined": true, "room": room}}, nil
}

// --- Room Leave ---

type wsRoomLeaveStep struct{ name string }

func newWSRoomLeaveStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsRoomLeaveStep{name: name}, nil
}

func (s *wsRoomLeaveStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "left": false}}, nil
	}

	connID, _ := current["connectionId"].(string)
	room, _ := current["room"].(string)

	h.leaveRoom(connID, room)
	return &sdk.StepResult{Output: map[string]any{"left": true, "room": room}}, nil
}

// --- Room List ---

type wsRoomListStep struct{ name string }

func newWSRoomListStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsRoomListStep{name: name}, nil
}

func (s *wsRoomListStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "connections": []string{}}}, nil
	}

	room, _ := current["room"].(string)
	members := h.roomMembers(room)
	if members == nil {
		members = []string{}
	}

	return &sdk.StepResult{Output: map[string]any{"connections": members, "count": len(members)}}, nil
}
