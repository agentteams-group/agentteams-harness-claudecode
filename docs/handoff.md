# Claude Code implementation handoff

Prepared on 2026-11-24. This is an implementation checklist, not an acceptance report.

## Starting point

- Repository: `agentteams-group/agentteams-harness-claudecode`
- Proposed runtime ID: `claudecode`
- Proposed image: `agentteams-harness-claudecode`
- Core inspected at `6ad1a9799` (main, Harness integration guide #1695). Resolve and pin the full commit when building; refresh main before starting implementation.
- The SDK language and supported role/deployment scope are for the implementation owner to declare. A shared image can select roles at startup.

## Native integration

1. Pin an official Claude Agent SDK release and its CLI. Validate actual tool registration, session persistence, interrupt and approval behavior. Use API-key authentication for hosted integration; confirm the selected gateway implements the SDK's model protocol.
2. Consume `agents/{workerIdentity}/runtime/runtime.yaml` and projected role assets. Resolve credentials from bindings; preserve the stable Worker identity. Reject unsupported configuration explicitly.
3. Register the role-specific schema from `plugins/teamharness/mcp/common/task_native.py`. Pass model arguments and framework context separately. Obtain the full SDK tool name from the real registration; do not implement Task mutations through arbitrary Bash commands.
4. Verify Controller coordination notifications before preparing planning, execution, review or completion scope. Use the actual Matrix event ID and authoritative role, Task, Team and process generation.
5. Journal tool admission before effects and save actual terminal receipts with original call IDs. Check prepared output/scratch paths before native writes. A tool error or interruption is not proof of no effect.
6. Persist native inputs and receipts before acknowledging them. Use the shared Matrix ledger and Task checkpoints for recovery; native sessions are optional caches. Discover recoverable Tasks at startup without requiring a new message.
7. Expose cancellation/steering and approval controls with requester, room and run binding. Keep the current permission policy for Bash, file and external MCP tools.
8. Package `agt`, shared libraries, TeamHarness, role assets and the official SDK in the candidate image. Add bounded runtime logs, readiness and real model/tool tracing without credential or content leakage.

## Core follow-up

Use a separate Core branch/PR for runtime validation, member-config projection, backend image selection, coordination/telemetry recognition and build references. Do not publish an invented release tag or enable an unbuilt default image. Keep Manager support disabled until implemented and verified.

## Acceptance

Follow the live guide's frozen-candidate rules. Cover role schema and native hooks with real SDK tests; then run simple/complex tasks (10 each), artifact checks, exact submission review, concurrency/duplicate delivery, cancellation and unknown outcomes. Worker support additionally requires restart recovery, both switch directions and a round trip with retained old sessions/cursors. Record Core/Harness SHAs, SDK/CLI versions, image digests and trace/Task/event links. Distinguish local tests, image smoke, real inference and admission certification in the report.

## Suggested layout

Choose a language before adding build tooling. Keep the native adapter under `src/`, launcher under `scripts/`, native/contract tests under `tests/`, and measured acceptance evidence summaries under `docs/`. Add a Dockerfile and CI once those entrypoints exist. The initial scaffold intentionally has no executable placeholder that could be mistaken for a supported runtime.

## Task context and completion follow-up

Use Core's `model_context` bridge for prepared execution and coordination contexts. Associated Skills must be present in the actual native instructions, including phases where the Skill tool is unavailable. The original conversation ends after intake; full planning runs in the Task room.

Match only the fixed registered Task tool's actual `PostToolUse` result. SDK 0.2.187 returns MCP content as a text-block list. A Core-validated `phaseComplete`/receipt closes continuation using `continue_=False`; verify the exact native JSONL tool result before accepting the terminal phase. The receipt and confirmed dispatch facts remain in native history without another generation or chat acknowledgement. Do not skip immutable report delivery when a direct-success node is already completed.

Assigned execution may use synchronous Bash through the existing permission callback. Journal admission and a verified terminal response; deny background commands, and leave interrupted or unresolved mutations for verification. Native fixture tests cover one model request at Task completion, journaled Bash, retained-result delivery and cross-runtime cache boundaries. These tests do not replace real Task/trace/attachment acceptance or full admission certification. The managed SDK disables automatic backgrounding. If Bash still returns a background task ID or an interrupted result, stop the model turn and retain the pending operation for verification.

A journaled in-flight tool sets Core execution to `needs_verification`. While the same generation, instance and process still own it, this is not ownership loss and must not trigger an SDK interrupt. Normal tool/inference admission still requires a verified terminal operation; a replacement process reconciles the retained receipt before continuing.

The matched Core contract is pinned to `ec78d76184a4db7db39cc6fb3c8282689ccd7a73` ([Core PR #1738](https://github.com/agentscope-ai/AgentTeams/pull/1738)); build the Controller/CLI and shared assets from that same revision. Local mixed normal and rework Tasks are separate from full admission certification.

For a verified planning wake, check the exact Matrix event against the Controller receipt and require a Chinese overview before dispatch. Publish the goal, requirements, acceptance criteria, assignments and selected outputs in the Task room. Keep the original source event for routing; use one execution-generation run ID for recovered tool activity and the saved stage result. Check the original Human conversation for the acceptance report and each selected file card. Open each card and verify its bytes against the accepted submission.

When Matrix dispatch finishes after a Worker has submitted, recover the pending event IDs from that exact native Task tool result. Verify the sender, node and Matrix event against the Controller. Record the confirmed fact in the submitting SDK session. In SDK 0.2.187 the UserPromptSubmit hook with `continue_=False` persists the input without requesting a model response. Check the stored message and verify a later normal resume sees it exactly once. The hook's `decision=block` does not preserve the input in the model's resumed context. Keep this receipt out of the Matrix chat timeline.
