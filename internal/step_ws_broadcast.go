package internal

import (
	"context"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type wsBroadcastStep struct {
	name string
}

func newWSBroadcastStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsBroadcastStep{name: name}, nil
}

func (s *wsBroadcastStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "recipients": 0}}, nil
	}

	room, _ := current["room"].(string)
	message, _ := current["message"].(string)
	exclude, _ := current["exclude"].(string)

	msg := []byte(message)
	var count int

	if room != "" {
		members := h.roomMembers(room)
		for _, id := range members {
			if id != exclude {
				if h.sendTo(id, msg) {
					count++
				}
			}
		}
	} else {
		h.mu.RLock()
		ids := make([]string, 0, len(h.connections))
		for id := range h.connections {
			if id != exclude {
				ids = append(ids, id)
			}
		}
		h.mu.RUnlock()
		for _, id := range ids {
			if h.sendTo(id, msg) {
				count++
			}
		}
	}

	return &sdk.StepResult{Output: map[string]any{"recipients": count}}, nil
}
