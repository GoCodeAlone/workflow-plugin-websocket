// Package workflowpluginwebsocket provides the WebSocket workflow plugin.
package workflowpluginwebsocket

import (
	"github.com/GoCodeAlone/workflow-plugin-websocket/internal"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// NewWebSocketPlugin returns the WebSocket SDK plugin provider.
func NewWebSocketPlugin() sdk.PluginProvider {
	return internal.NewWebSocketPlugin()
}

// Hub is an exported interface for the WebSocket hub's room and broadcast capabilities.
// Satisfied by the internal hub after the ws.server module is initialized.
type Hub interface {
	BroadcastToRoom(room string, msg []byte) int
	JoinRoom(connID, room string) bool
	LeaveRoom(connID, room string)
}

// GetHub returns the global WebSocket hub once the ws.server module has been
// initialized. Returns nil if the module has not started yet.
func GetHub() Hub {
	if h := internal.GetHub(); h != nil {
		return h
	}
	return nil
}
