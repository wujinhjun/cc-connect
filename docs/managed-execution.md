# Durable managed execution webhook

The ordinary webhook may acknowledge before discovering that a conversation is
busy. Automation requiring execution receipts must opt into the managed inbox:

```toml
[webhook]
enabled = true
port = 9111
path = "/hook"
token = "<securely injected shared secret>"
execution_state = "/path/to/private/executions.json"
```

The parent directory must be private (0700). The state file and exclusive OS
lock use 0600. Linux, macOS and FreeBSD are supported. Configuration is loaded
and orphaned sessions are fenced before messaging platforms start. The legacy
`/hook` behavior is unchanged; there is no fallback from the managed API.

POST `/hook/execution` with a Bearer header. Query-string authentication is not
accepted. Version-1 envelopes contain `operation`, `route` and `task`:

```json
{
  "version": 1,
  "operation": "deliver",
  "route": {"provider": "cc-connect", "target": "reviewer", "session": "discord:123"},
  "task": {
    "task_id": "stable-task", "attempt_id": "unique-round", "attempt": 1,
    "repository": "owner/repo", "pull_number": 42
  },
  "prompt": "Review this exact commit using the configured GitHub identity."
}
```

The full task JSON and route are retained and compared for duplicate requests.
Additional task fields such as SHA, role, source and identity are opaque to core.

- `deliver`: persist admission before acknowledging; duplicate rounds reuse the
  existing receipt. A conflicting payload/target, stale attempt number or other
  unresolved round for the same repository/PR is rejected. Busy conversations
  keep accepted work in FIFO order rather than dropping it.
- `status`: return the exact round's durable `receipt`. A missing record is 404,
  never proof of termination. Diagnostic `error` explains unresolved runtimes.
- `stop`: persist the request and fence delayed delivery. A queued round that
  never dispatched becomes stopped immediately. A running/dispatching round
  remains nonterminal until its runtime is actually joined. A stop for an unseen
  round creates a tombstone, preventing a later deliver from resurrecting it.
- `resolve`: a trusted operator's explicit stop/join assertion, with nonempty
  `reason`. It can reconcile an orphan or create a tombstone for a missing round;
  it refuses an active local worker. It does not kill an external process.
- `notify`: persist a diagnostic (`notice_id`, `prompt`, `route`) without starting
  an agent. Platform-send failures retry; duplicate IDs do not create another
  coding task. A crash after sending but before committing can repeat a notice.

Receipts contain `task_id`, `attempt_id`, monotonic `sequence`, `state`, and
start/heartbeat/end timestamps. Queue admission reports `queued`, with a queue
heartbeat and no start ACK. A dispatch intent is fsynced before runtime creation;
`running` is published only after AgentSession.Send accepts the prompt. Runtime
completion does not determine GitHub approval or other business results.

Session ownership survives the engine's ordinary unlock and stale-lock recovery.
The session fence is released only after confirmed runtime termination AND a
durable terminal receipt. Codex terminates/joins managed process groups; Pi RPC
terminates/joins its RPC process group, including concurrent-close protection.
Other runtimes, including Pi JSON, are rejected before admission until their
adapters implement the optional managed-execution capabilities. Tasks must not
escape these process groups or leave detached external workloads.

An interrupted running/dispatching record is orphaned on restart. It blocks new
managed work for the PR and quarantines its conversation until operator
resolution. Timeouts and missing PIDs never authorize replay. Destructive
conversation commands cannot bypass queued/running ownership. While a managed
round runs, new ordinary turns are refused to avoid losing messages in the
response/termination gap; /status, /history, /stop, permission callbacks and /ps
remain available. Different PRs sharing a working directory need additional
workspace isolation by the workflow owner.

Ink Commons enables this endpoint with `cc_connect_execution: true`, pulls
receipts, and maintains its own persistent review/fix workflow and alert outbox.
Upgrade both components before enabling it; historical legacy webhook requests
need manual stop/fencing because they never carried managed execution IDs.

## Verification

Build the normal web assets first when `web/dist` is absent:

```sh
(cd web && npm install --package-lock=false && npm run build)
```

```sh
go test ./...
go test -race ./core ./agent/codex ./agent/pi ./config
go test ./core -run TestCUJ
go build ./...
go vet ./...
```

On macOS use a canonical temporary path (for example `TMPDIR=/private/tmp`) for
existing fixtures that compare subprocess working-directory paths.

`tests/ink-managed-execution` is a model-free executable fixture for the companion
Ink Commons tagged integration test. It exercises the real inbox, engine and
session locks, mocking only AgentSession, platform I/O and GitHub. The combined
scenario covers wrong review identity, daemon restart, bounded follow-up, busy
developer queueing, a new SHA and exact-head approval.
