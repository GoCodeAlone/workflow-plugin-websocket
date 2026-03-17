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

	// globalWSConnectHandler fires once per new connection, before the first message.
	// extra carries URL query params extracted at upgrade time (e.g. sessionId → sessionID).
	globalWSConnectHandler   func(connID string, extra map[string]any)
	globalWSConnectHandlerMu sync.RWMutex

	// globalWSDisconnectHandler fires when a connection closes.
	globalWSDisconnectHandler   func(connID string)
	globalWSDisconnectHandlerMu sync.RWMutex
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

func setGlobalWSConnectHandler(h func(connID string, extra map[string]any)) {
	globalWSConnectHandlerMu.Lock()
	defer globalWSConnectHandlerMu.Unlock()
	globalWSConnectHandler = h
}

// callGlobalWSConnectHandler is called by wsServerModule.ServeHTTP after a new
// connection is registered in the hub. extra carries URL query params from the
// upgrade request so pipelines like ws_connect can reference e.g. {{ .sessionID }}.
func callGlobalWSConnectHandler(connID string, extra map[string]any) {
	globalWSConnectHandlerMu.RLock()
	h := globalWSConnectHandler
	globalWSConnectHandlerMu.RUnlock()
	if h != nil {
		h(connID, extra)
	}
}

func setGlobalWSDisconnectHandler(h func(connID string)) {
	globalWSDisconnectHandlerMu.Lock()
	defer globalWSDisconnectHandlerMu.Unlock()
	globalWSDisconnectHandler = h
}

// callGlobalWSDisconnectHandler is called by connection.readPump when a
// connection's read loop exits (i.e. the client disconnected).
func callGlobalWSDisconnectHandler(connID string) {
	globalWSDisconnectHandlerMu.RLock()
	h := globalWSDisconnectHandler
	globalWSDisconnectHandlerMu.RUnlock()
	if h != nil {
		h(connID)
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
//
// Connect/disconnect lifecycle events are also fired:
//
//	event "connect"    — fired immediately after the connection is registered;
//	                     only connectionId is populated (no message/payload).
//	event "disconnect" — fired when the read loop exits (client gone).
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

	setGlobalWSConnectHandler(func(connID string, extra map[string]any) {
		if t.cb == nil {
			return
		}
		ctx := map[string]any{
			"connectionId": connID,
		}
		for k, v := range extra {
			ctx[k] = v
		}
		_ = t.cb("connect", ctx)
	})

	setGlobalWSDisconnectHandler(func(connID string) {
		if t.cb == nil {
			return
		}
		_ = t.cb("disconnect", map[string]any{
			"connectionId": connID,
		})
	})

	return nil
}

func (t *wsTrigger) Stop(_ context.Context) error {
	setGlobalWSMessageHandler(nil)
	setGlobalWSConnectHandler(nil)
	setGlobalWSDisconnectHandler(nil)
	return nil
}
