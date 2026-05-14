# Strict-Contracts Adoption — workflow-plugin-websocket

**Date:** 2026-05-13
**Issue:** #5
**Branch:** feat/issue-5-strict-contracts

## Summary

Add strict-proto contract support to workflow-plugin-websocket, enabling the
workflow engine to validate plugin messages at the gRPC boundary using typed
protobuf messages instead of `map[string]any` dispatch.

## Surface

| Kind    | Type           | Message(s)                                |
|---------|---------------|-------------------------------------------|
| module  | ws.server      | WSServerConfig                            |
| step    | step.ws_send   | WSSendConfig / WSSendInput / WSSendOutput |
| step    | step.ws_close  | WSCloseConfig / WSCloseInput / WSCloseOutput |
| trigger | websocket      | WebSocketTriggerConfig                    |

## Files Added

- `proto/websocket/v1/websocket.proto` — source of truth for all typed messages
- `gen/websocket.pb.go` — generated from proto (do not hand-edit)
- `internal/contracts.go` — ContractRegistry wired to wsPlugin
- `plugin.contracts.json` — static contract manifest read by wfctl --strict-contracts

## Design Decisions

1. **Separate Config/Input/Output per step** — follows tournament/worldengine pattern
   (not the shared SalesforceStepInput flattening), because ws_send and ws_close have
   distinct, small, typed fields where separate messages are cleaner.

2. **Trigger has an empty config message** — `WebSocketTriggerConfig` has no fields;
   the trigger attaches to the global hub automatically. An empty message is the
   correct approach (not omitting the config contract) so wfctl can still validate
   that no unexpected config keys are passed.

3. **WSSendInput/WSCloseInput use Struct** — runtime inputs beyond config fields
   are represented as `google.protobuf.Struct data = 1;` matching the pattern used
   by tournament and worldsim, since the workflow engine populates step input from
   trigger data and prior step outputs (free-form).

## Assumptions

- The workflow engine at v0.51.7 supports `CONTRACT_KIND_TRIGGER` (verified in plugin.pb.go).
- `protodesc.ToFileDescriptorProto` is the correct way to register the file descriptor
  (matches all precedent plugins).
- The `wsPlugin` type in `internal/plugin.go` is the receiver for `ContractRegistry()`.
- No changes to step execution logic are required — strict contracts only add type
  metadata at the plugin boundary; runtime behavior is unchanged.

## Rollback

No runtime behavior changed. The `plugin.contracts.json` and `internal/contracts.go`
are additive. To roll back: revert the commit and rebuild. The engine falls back to
legacy struct mode if the plugin does not implement `ContractProvider`.
