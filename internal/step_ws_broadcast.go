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

	room, _ := config["room"].(string)
	message, _ := config["message"].(string)
	exclude, _ := config["exclude"].(string)

	msg := []byte(message)
	var count int
	broadcastHook := getBroadcastHook()
	sendHook := getSendHook()

	if room != "" {
		if broadcastHook != nil && exclude == "" {
			count = broadcastHook(room, msg)
		} else {
			members := h.roomMembers(room)
			for _, id := range members {
				if id != exclude {
					var sent bool
					if sendHook != nil {
						sent = sendHook(id, msg)
					} else {
						sent = h.sendTo(id, msg)
					}
					if sent {
						count++
					}
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
			var sent bool
			if sendHook != nil {
				sent = sendHook(id, msg)
			} else {
				sent = h.sendTo(id, msg)
			}
			if sent {
				count++
			}
		}
	}

	return &sdk.StepResult{Output: map[string]any{"recipients": count}}, nil
}
