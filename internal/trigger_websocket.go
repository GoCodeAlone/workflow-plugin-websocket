package internal

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// globalWSMessageHandler is the package-level dispatch hook.
// wsServerModule calls callGlobalWSMessageHandler for every inbound WS message,
// allowing the websocket trigger module to receive them without holding a direct
// reference to the wsServerModule instance.
var (
	globalWSMessageHandler   func(connID string, msg []byte)
	globalWSMessageHandlerMu sync.RWMutex
)

func setGlobalWSMessageHandler(h func(connID string, msg []byte)) {
	globalWSMessageHandlerMu.Lock()
	defer globalWSMessageHandlerMu.Unlock()
	globalWSMessageHandler = h
}

// callGlobalWSMessageHandler is called by wsServerModule.ServeHTTP for each
// inbound message. It is safe for concurrent use.
func callGlobalWSMessageHandler(connID string, msg []byte) {
	globalWSMessageHandlerMu.RLock()
	h := globalWSMessageHandler
	globalWSMessageHandlerMu.RUnlock()
	if h != nil {
		h(connID, msg)
	}
}

// wsTrigger implements sdk.ModuleInstance for the "websocket" trigger type.
// The host engine creates this via CreateModule("websocket", ...) and calls
// Init/Start/Stop to manage its lifecycle (same pattern as RemoteTrigger).
//
// When Start is called, it installs itself as the global WS message handler.
// Each incoming message is forwarded to the TriggerCallback with fields:
//
//	connectionId — the connection UUID
//	message      — raw UTF-8 message string
//	payload      — decoded JSON object (if the message is valid JSON)
//	room         — first room the connection belongs to (empty if none)
//	rooms        — all rooms the connection belongs to
type wsTrigger struct {
	cb sdk.TriggerCallback
}

// newWSTrigger creates a websocket trigger with an optional callback.
// When used as an external plugin module (CreateModule), cb is nil and the
// trigger fires only via the global handler hook (useful for local testing).
// When used via TriggerProvider.CreateTrigger, cb is set by the SDK.
func newWSTrigger(_ map[string]any, cb sdk.TriggerCallback) (sdk.ModuleInstance, error) {
	return &wsTrigger{cb: cb}, nil
}

func (t *wsTrigger) Init() error { return nil }

func (t *wsTrigger) Start(_ context.Context) error {
	setGlobalWSMessageHandler(func(connID string, msg []byte) {
		if t.cb == nil {
			return
		}

		// Look up rooms this connection belongs to.
		var room string
		var rooms []string
		if h := GetHub(); h != nil {
			h.mu.RLock()
			if connRooms, ok := h.connRooms[connID]; ok {
				for r := range connRooms {
					rooms = append(rooms, r)
					if room == "" {
						room = r
					}
				}
			}
			h.mu.RUnlock()
		}

		data := map[string]any{
			"connectionId": connID,
			"message":      string(msg),
			"room":         room,
			"rooms":        rooms,
		}

		// Merge decoded JSON payload if message is valid JSON.
		var payload map[string]any
		if err := json.Unmarshal(msg, &payload); err == nil {
			data["payload"] = payload
		}

		_ = t.cb("message", data)
	})
	return nil
}

func (t *wsTrigger) Stop(_ context.Context) error {
	setGlobalWSMessageHandler(nil)
	return nil
}
