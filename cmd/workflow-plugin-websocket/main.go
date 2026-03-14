package main

import (
	"github.com/GoCodeAlone/workflow-plugin-websocket/internal"
	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func main() {
	sdk.Serve(internal.NewWebSocketPlugin())
}
