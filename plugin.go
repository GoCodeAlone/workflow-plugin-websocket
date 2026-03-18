// Package workflowpluginwebsocket provides the WebSocket workflow plugin.
package workflowpluginwebsocket

import (
	"context"
	"net/http"

	"github.com/GoCodeAlone/workflow-plugin-websocket/internal"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// NewWebSocketPlugin returns the WebSocket SDK plugin provider.
func NewWebSocketPlugin() sdk.PluginProvider {
	return internal.NewWebSocketPlugin()
}

// Hub is an exported interface for the WebSocket hub's broadcast capabilities.
// Satisfied by the internal hub after the ws.server module is initialized.
type Hub interface {
	BroadcastToRoom(room string, msg []byte) int
	// SendTo delivers a text frame directly to a connection by its ID.
	SendTo(connID string, msg []byte) bool
	// SendBinary delivers a binary frame directly to a connection by its ID.
	SendBinary(connID string, msg []byte) bool
	// BroadcastBinaryToRoom sends a binary frame to all connections in room.
	BroadcastBinaryToRoom(room string, msg []byte) int
}

// GetHub returns the global WebSocket hub once the ws.server module has been
// initialized. Returns nil if the module has not started yet.
func GetHub() Hub {
	if h := internal.GetHub(); h != nil {
		return h
	}
	return nil
}

// WSServerModule is the public interface for the ws.server module instance.
// It exposes the HTTP handler methods needed to register the WebSocket upgrade
// endpoint directly on the host engine's HTTP router.
type WSServerModule interface {
	sdk.ModuleInstance
	// ServeHTTP handles WebSocket upgrade requests.
	ServeHTTP(w http.ResponseWriter, r *http.Request)
	// HTTPPath returns the path this module listens on (e.g. "/ws").
	HTTPPath() string
}

// NewWSServerModule creates a ws.server module instance that can be registered
// directly on the host engine's HTTP router.  This is the correct way to embed
// the WebSocket plugin in-process: HTTP upgrade requests cannot cross the gRPC
// boundary used by external plugins.
func NewWSServerModule(name string, config map[string]any) (WSServerModule, error) {
	inst, err := internal.NewWSServerModule(name, config)
	if err != nil {
		return nil, err
	}
	return inst, nil
}

// WSServerHTTPHandler is an http.Handler adapter for WSServerModule.
// It satisfies the workflow engine's module.HTTPHandler interface via
// module.NewHTTPHandlerAdapter.
type WSServerHTTPHandler struct {
	module WSServerModule
}

// NewWSServerHTTPHandler wraps a WSServerModule as a standard http.Handler.
func NewWSServerHTTPHandler(m WSServerModule) http.Handler {
	return &WSServerHTTPHandler{module: m}
}

// ServeHTTP delegates to the underlying WSServerModule.
func (h *WSServerHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.module.ServeHTTP(w, r)
}

// StartCtx satisfies context-aware start (no-op wrapper).
func StartCtx(ctx context.Context, m WSServerModule) error {
	return m.Start(ctx)
}
