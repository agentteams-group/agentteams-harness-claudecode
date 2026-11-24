# AgentTeams Claude Code Harness

Integration repository for running Claude Code through the official Claude Agent SDK as an AgentTeams Harness.

This experimental managed Harness runs Manager, Leader and Worker roles with the runtime ID `claudecode`. Each role uses the Python Claude Agent SDK and its bundled Claude Code CLI. A deployment needs an Anthropic Messages-compatible gateway route and unencrypted Matrix rooms. Real-session admission of this candidate is in progress.

## Build and test

Check out AgentTeams beside this repository. Use the Core revision below for the shared libraries, role assets and `agt`. Build the Controller image from the same Core revision and set `CONTROLLER_IMAGE` to its image reference.

```sh
git -C ../AgentTeams checkout f2d5424f09976b1c2703a1732c64d19c93fcff6b
git -C ../AgentTeams submodule update --init deepseek-harness
docker buildx build --load --build-context agentteams=../AgentTeams \
  --build-arg AGENTTEAMS_CONTROLLER_IMAGE="$CONTROLLER_IMAGE" \
  -t agentteams/agentteams-harness-claudecode:dev .
```

With Python 3.12 in a virtual environment, run the native regression and wire tests. The wire test starts the bundled Claude CLI, a local Messages endpoint and a local Matrix fixture.

```sh
python -m pip install -r requirements.txt
AGENTTEAMS_CORE_DIR="$(cd ../AgentTeams && pwd)" python -m unittest discover -s tests
```

## Integration boundary

Core owns the public `task/v1` schema, authorization, state transitions and artifact delivery. This Harness projects the member contract into the SDK, admits Matrix events and journals native tool calls. Workers discover recoverable Tasks on startup, verify terminal tool receipts and prepare versioned upstream inputs. A compatible native session can resume after a restart. The public checkpoint supplies confirmed work when the session is missing or stale.

## Matrix interaction

The adapter uses the existing AT wire contracts; it does not require a Claude-specific Dashboard parser.

| Native event | Matrix projection |
| --- | --- |
| Thinking fragments | `org.agentteams.run` v1 `thinking` blocks, edited in place and closed when streaming ends |
| Tool start / return / failure | Flat v1 `tool_call` blocks with stable call IDs, parameters, results, timestamps and truthful status |
| Tool permission request | `org.agentteams.approval` with `reply_format: com.agentteams.answer` and a per-request context |
| `AskUserQuestion` | v1 `confirmation` with single/multiple selection and custom answers |
| Direct execution / stop / steer | `com.agentteams.execution` / `com.agentteams.control`, bound to the original room, requester and run |

Process messages carry `org.agentteams.progress_only` so they cannot reopen another Agent turn. Task coordination phases suppress detailed thinking according to the shared Task contract. Tool-message filtering does not hide human approval requests.

Native permission requests pause until the requester or Team admin answers. Decisions are one-shot; wrong-room, wrong-agent, stale-context and Agent-authored answers are ignored. Requests expire after `AGENTTEAMS_HITL_TIMEOUT_SECONDS` (300 seconds by default). Restarted processes retire the old cards rather than approving them. Terminal question/approval receipts are persisted until Matrix acknowledges them.

In a verified two-member Human conversation, Dashboard Stop calls native `interrupt()` and publishes `stopped` after SDK teardown. Steer sends the supplementary Matrix input into the running SDK session. Its native input UUID must appear in a result receipt before the supplement is acknowledged, including when it races with the original response. Plain Matrix clients can use `/cancel`, `/steer text`, and the context-bound `/approve`, `/reject` or `/answer` commands printed in the request message.

Ordinary Worker conversations expose Bash with native one-shot approval. Explicit `AGENTTEAMS_YOLO=1` retains the ordinary-turn Bash bypass. Declared external HTTP MCP tools use native approval. Managed Task execution retains the existing admitted Read/Write/Edit policy; it does not enable unjournaled Bash or external MCP effects. Nested agents remain disabled.

## Restart and Harness handoff

Managed Workers use the Controller's process, instance and generation ownership checks before tools and periodically during inference. Cancellation or replacement interrupts the old SDK; a late result cannot become a synthetic submission. Missing, expired, incompatible or foreign-runtime native caches resume from the public checkpoint. Claude never interprets another Harness's private session format.

Successful native file mutations stage an immutable byte copy and terminal receipt in the member's runtime directory before uploading artifacts. If an upload fails, recovery resends those saved bytes and receipts instead of repeating the mutation. Preserve the member runtime volume across process restarts; committed remote checkpoints and outputs support container replacement. Unknown effects without a verifiable receipt remain fenced for verification.

The tests cover shared checkpoint continuation, foreign-cache rejection, old-owner isolation, saved-reply delivery, upload recovery, and actual CLI approval/question/stop/steer/streaming behavior against local Matrix and Messages fixtures. These checks do not replace live same-task restart, both handoff directions, heterogeneous Leader/Worker and performance acceptance in the target deployment.

See the [Core deployment guide](https://github.com/agentscope-ai/AgentTeams/blob/f2d5424f09976b1c2703a1732c64d19c93fcff6b/docs/usage/integrations/claude-code.md), [integration handoff](docs/handoff.md) and [Harness admission guide](https://github.com/agentscope-ai/AgentTeams/blob/main/docs/design/harness-integration-guide.md).

## Model request tracing

With AgentTeams tracing enabled, count model attempts using `claude-code.model.request`. The Harness exports one child span per request recorded by the pinned Claude CLI, including HTTP retries and the fallback from streaming to a non-streaming request. It reads the current turn’s native journal after SDK teardown, including when the Harness times out. A resumed turn skips requests from earlier turns.

`agentteams.model.request.id` identifies each attempt. A terminal assistant reply supplies `gen_ai.response.id`. An attempt without a terminal reply has error status. Its end time is the next observed request or the end of the turn, as identified by `agentteams.model.end_observation`. These times give an observation boundary for diagnosing a stall. The existing `claude-code.model.response` spans describe SDK response messages. Count a successful generation once by its response ID. Request spans contain metadata only.

## References

- [Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk/overview)
- [AgentTeams Core](https://github.com/agentscope-ai/AgentTeams)
- [Member runtime configuration](https://github.com/agentscope-ai/AgentTeams/blob/main/docs/design/member-runtime-config-contract.md)
- [Qoder sibling integration](https://github.com/agentteams-group/agentteams-harness-qoder)

Licensed under Apache-2.0; see [LICENSE](LICENSE).
