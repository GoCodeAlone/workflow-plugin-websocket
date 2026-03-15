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
