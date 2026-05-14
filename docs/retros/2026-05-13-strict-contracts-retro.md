# Retro: Strict-Contracts Adoption — workflow-plugin-websocket

**Date:** 2026-05-13
**PR:** #6 — merged at 04b29bc9cf118e6b738b532d9fcb47c6c95f02dd
**Issue:** #5

## What Was Done

Added strict-proto contract support across all 4 plugin surfaces:
- **ws.server** module (WSServerConfig)
- **step.ws_send** (WSSendConfig/Input/Output)
- **step.ws_close** (WSCloseConfig/Input/Output)
- **websocket** trigger (WebSocketTriggerConfig)

Files: `proto/websocket/v1/websocket.proto`, `gen/websocket.pb.go`,
`internal/contracts.go`, `internal/contracts_test.go`, `plugin.contracts.json`.

## Gates

| Gate | Result | Notes |
|------|--------|-------|
| `go build ./...` | PASS | Local, GOWORK=off |
| `go vet ./...` | PASS | Local |
| `go test -race ./internal/...` | PASS | 17 tests (15 pre-existing + 2 new) |
| Copilot review | PASS | 5 comments; 1 real (contracts test) addressed; 2 ghost flags; 2 stylistic (optional string — deferred per precedent) |
| CodeQL CI | PASS | 2/2 checks |
| CI test + strict-contracts | Pre-existing failure | `actions/checkout@v6` is not a valid tag; same failure on main pre-PR |

## What Worked

- **Reading precedent implementations first** prevented inventing types. All proto messages
  were derived directly from `internal/module_ws_server.go`, `step_ws_send.go`,
  `step_ws_close.go`, and `trigger_websocket.go` field names.
- **Tournament pattern (separate Config/Input/Output per step)** was the right choice —
  ws_send and ws_close have distinct, small config shapes that don't benefit from a
  shared input wrapper.
- **Empty trigger config message** is the correct approach for the websocket trigger
  (no required fields; trigger is self-wiring via global hub).
- **Compile-time ContractProvider assertion** caught interface compliance immediately.

## What Didn't Work

- **Copilot ghost flags** on markdown (line 20 table, line 57 backtick) were false positives —
  comment anchoring shifted to old commit SHA. File content was correct.
- **CI workflow pre-existing failure** (`actions/checkout@v6`) obscured test results.
  This is a repo-level infra gap predating this PR and not part of this scope.

## Deferred

- `optional string error` — Copilot suggested this for `WSSendOutput.error` and
  `WSCloseOutput.error` to distinguish "not set" vs empty string. Deferred: all
  precedent plugins (tournament, salesforce, worldsim) use plain `string error`,
  and the runtime already uses empty-string-as-absence semantics consistently.
  Could be addressed in a future strict-contracts polish pass across all plugins.
- CI workflow fix — `actions/checkout@v6` → valid version.
