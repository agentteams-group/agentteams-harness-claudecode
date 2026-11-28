"""Project one AT member turn onto the public Claude Code Agent SDK."""
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

import anyio
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher, ResultMessage, SystemMessage, AssistantMessage
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny, StreamEvent
import yaml
from opentelemetry import propagate, trace
from telemetry import identity_attributes, model_requests, set_io, turn_span
from task_recovery import Recovery
from worker_files import persist_terminal, stage_output
from agentteams_skill_warnings import check_skill, report_warnings
from interaction import TurnStopped
from responses_compat import model_endpoint
from mcp_transport import observed_servers


class UnverifiedToolEffect(RuntimeError):
    """The Task journal needs evidence before the model can continue."""


def role_of(record):
    role = record["member"]["role"]
    role = {"standalone": "worker", "team_leader": "leader", "remote": "remote-member"}.get(role, role)
    if role not in {"manager", "leader", "worker", "remote-member"}:
        raise ValueError("Unsupported member role")
    return role


def prepare_assets(record, bundle, plugin):
    manifest = yaml.safe_load((bundle / "plugin.yaml").read_text())
    role = role_of(record)
    paths = manifest["prompts"]
    prompts = list(paths["manager"].values()) if role == "manager" else [
        paths["team"], paths["agent"]["remoteMember" if role == "remote-member" else role]]
    context = [(bundle / path).read_text() for path in prompts]
    context.append((bundle / "skills/team/task-management/SKILL.md").read_text())
    context.append(json.dumps({key: record.get(key) for key in ("member", "team")}, ensure_ascii=False))
    context.extend((record["desired"].get("inlineConfig") or {}).values())
    skills = plugin / "skills"
    skills.mkdir()
    for section in manifest["skills"].values():
        for entry in section:
            if role in entry["roles"]:
                (skills / entry["id"]).symlink_to((bundle / entry["path"]).resolve())
    warnings = []
    # The skill plugin is rebuilt for this turn, so removing an assignment
    # cannot leave a discoverable stale copy in a long-lived session.
    builtin = {entry["id"] for section in manifest["skills"].values() for entry in section}
    for name in sorted(set(record["desired"].get("skills") or []) - builtin):
        try:
            if not name or name in {".", ".."} or Path(name).name != name or "\\" in name:
                raise ValueError("Invalid skill ID")
            destination = skills / name
            prefix = os.getenv("AGENTTEAMS_STORAGE_PREFIX")
            if prefix:
                remote = f"{prefix.rstrip('/')}/{record['storage']['memberPrefix']}/skills/{name}/"
                subprocess.run(["mc", "mirror", "--overwrite", remote, str(destination)], check=True,
                               timeout=30, capture_output=True)
            else:
                root = Path(os.environ["TEAMHARNESS_RUNTIME_CONFIG"]).parent.parent / "skills"
                source = root / name
                if not source.resolve().is_relative_to(root.resolve()):
                    raise ValueError("Invalid skill path")
                check_skill(source)
                shutil.copytree(source, destination)
            check_skill(destination)
            context.append((destination / "SKILL.md").read_text())
        except (ValueError, OSError, subprocess.SubprocessError) as error:
            if name and Path(name).name == name and name not in {".", ".."}:
                shutil.rmtree(skills / name, ignore_errors=True)
            warnings.append(f"Skill {name}: load failed ({type(error).__name__})")
    report_warnings(warnings)
    return "\n\n".join(filter(None, context))


def mcp_servers(record, bundle, provider, task, plugin):
    # stdio MCP children receive a filtered environment from Claude Code.
    env = {k: v for k, v in os.environ.items() if k.startswith(("AGENTTEAMS_", "TEAMHARNESS_", "MC_"))}
    context_file = plugin / "task-context.json"
    context_file.write_text(json.dumps(task, default=str))
    env.update(AGENTTEAMS_PLUGIN_DIR=str(bundle), AGENTTEAMS_AGENT_ROLE=role_of(record),
               AT_CLAUDE_TASK_CONTEXT_FILE=str(context_file))
    if run_id := task.get("runId") or task.get("sourceEvent"):
        env["AGENTTEAMS_RUN_ID"] = run_id
    carrier = {}
    propagate.inject(carrier)
    if carrier.get("traceparent"):
        env["AT_OTEL_TRACEPARENT"] = carrier["traceparent"]
    servers = {"teamharness": {"command": sys.executable,
                               "args": [str(Path(__file__).with_name("team_tools.py"))], "env": env}}
    if any(task.get(key) for key in ("review", "planning", "completion")):
        return servers
    for item in record["desired"].get("mcpServers") or []:
        name, url = item["name"], item["url"]
        if not name or name in servers or item.get("transport", "http") not in {"http", "streamable-http"}:
            raise ValueError("Invalid or duplicate HTTP MCP server")
        address = urlsplit(url)
        if address.scheme not in {"https", "http"} or not address.netloc or address.username is not None:
            raise ValueError("Expected HTTP(S) MCP URL")
        headers = {}
        gateway = urlsplit(provider["baseUrl"])
        if (address.scheme, address.netloc) == (gateway.scheme, gateway.netloc):
            headers["Authorization"] = "Bearer " + provider["apiKey"]
        servers[name] = {"type": "http", "url": url, "headers": headers}
    return servers


def input_directories(context):
    if not context.get("taskDirectory"):
        return []
    root = (Path(context["taskDirectory"]) / "inputs").resolve()
    return [path for item in context.get("inputs", []) if item.get("readOnly") and item.get("directory")
            for path in [Path(item["directory"]).resolve()] if path != root and path.is_relative_to(root)]


def file_allowed(tool, fields, workspace, skills, context):
    path = (workspace / (fields.get("file_path") or fields.get("path") or ".")).resolve()
    pattern = Path(fields.get("pattern") or "") if tool == "Glob" else Path()
    if pattern.is_absolute() or ".." in pattern.parts:
        return False
    if path.is_relative_to(workspace.resolve()):
        return True
    if tool not in {"Read", "Grep", "Glob"}:
        return False
    return any(path.is_relative_to(root) for root in [*input_directories(context), *(s.resolve() for s in skills)])


async def run_turn(record, provider, workspace, prompt, session, task, bundle, *, on_effect=None, on_checkpoint=None, interaction=None, message_identity=None):
    from runtime_control import approval_mode
    mode = approval_mode(record)
    model = provider["models"][0]["id"]
    if (record["desired"].get("agentPackage") or {}).get("ref"):
        raise ValueError("Claude Code does not support AgentSpec packages")
    with (turn_span(role_of(record), task, model, input=prompt) as span,
          model_requests(span, workspace, session) as new_session_id,
          tempfile.TemporaryDirectory(prefix="at-claude-skills-") as directory):
        plugin = Path(directory)
        instructions = prepare_assets(record, bundle, plugin)
        if task.get("execution"):
            instructions += "\n读取和修改工作文件时使用 Read、Write 和 Edit。需要验证文件内容时可用已安装的 Python 3。使用其他命令行工具前先确认其可用性。"
        if task.get("planning"):
            instructions += "\n先从当前主任务核对用户指定的节点数、负责人和依赖；逐项按约定规划。领域 Skill 的默认流程只补充用户未指定的部分。规划时在 task plan 的 overview 中写明目标、要求、验收标准、节点分工和最终产物。保存后系统会先发布概要，再派单。"
        servers = mcp_servers(record, bundle, provider, task, plugin)
        refreshed = plugin / "refreshed.json"
        servers["teamharness"]["env"]["AT_CLAUDE_REFRESH_FILE"] = str(refreshed)
        decision = any(task.get(key) for key in ("review", "planning", "completion"))
        tools = ["AskUserQuestion"] if decision else ["Read", "Glob", "Grep", "Skill", "AskUserQuestion"]
        if not decision:
            tools.extend(["Write", "Edit", "Bash"])
        context = task.get("context") or {}
        identity = message_identity or task.get("identity")
        recovery = Recovery(device_delivery=True) if identity and not message_identity else None
        matrix = None
        if message_identity:
            from worker_matrix_state import WorkerMatrix
            matrix = WorkerMatrix('claudecode', device_delivery=True)
        journal = recovery is not None or matrix is not None

        def admit_tools():
            return matrix.message_current(identity) if matrix else recovery.tools_current(identity)

        def begin_tools(calls):
            return matrix.message_begin_tools(identity, calls) if matrix else recovery.begin_tools(identity, calls)

        def finish_tool(operation, result, artifacts=()):
            if matrix:
                return matrix.message_finish_tools(identity, operation, [result])
            return recovery.finish_tools(identity, operation, [result], artifacts=artifacts)
        operations = {}
        remote_calls, remote_returns, remote_sessions = {}, {}, {}
        tool_spans = {}
        confirmed = None

        def remember_confirmed():
            nonlocal confirmed
            if confirmed and on_checkpoint and not (interaction and interaction.stopped):
                try:
                    if on_checkpoint(*confirmed):
                        confirmed = None
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    # Native sessions are caches; their storage cannot replace
                    # the current execution admission or mask its actual result.
                    span.add_event("native_session.cache_unavailable", {"error.type": type(error).__name__})

        def remote_name(name):
            return next(((server, name[len('mcp__' + server + '__'):]) for server, config in servers.items()
                         if config.get('type') == 'http' and name.startswith('mcp__' + server + '__')), None)

        def reserve_remote(server, name, arguments):
            if not journal:
                return None
            matches = [key for key, call in remote_calls.items()
                       if key in operations and call == (server, name, arguments)]
            if len(matches) != 1:
                raise ValueError('MCP request does not match its admitted native call')
            return matches[0]

        async def returned_remote(key, response):
            nonlocal confirmed
            remote_returns[key] = response
            result = {'id': key, 'content': response}
            if response.get('httpStatus', 200) >= 400 or 'error' in response or (response.get('result') or {}).get('isError') is True:
                result['status'] = 'error'
            try:
                saved = await asyncio.to_thread(finish_tool, operations[key], result)
            except (OSError, ValueError, subprocess.SubprocessError):
                # Shared recovery retains the return. Still forward the actual
                # upstream bytes to the CLI, then stop before further effects.
                task['_unverified_tool'] = 'mcp__' + remote_calls[key][0] + '__' + remote_calls[key][1]
            else:
                if remote_sessions.get(key):
                    confirmed = (remote_sessions[key], key, saved["execution"])
                del operations[key]

        async def before_tool(data, call_id, _context):
            nonlocal confirmed
            name = data["tool_name"]
            fields = data.get("tool_input") or {}
            try:
                remember_confirmed()
                if interaction and interaction.stopped:
                    raise ValueError("This execution has stopped")
                if on_effect and name not in {"Read", "Glob", "Grep", "Skill", "AskUserQuestion"}:
                    on_effect()
                if name == "Bash" and fields.get("run_in_background"):
                    raise ValueError("Background commands require an asynchronous terminal receipt adapter")
                if name in {"Read", "Write", "Edit", "Glob", "Grep"} and not file_allowed(
                        name, fields, workspace, (plugin / "skills").iterdir(), context):
                    raise ValueError("Outside the prepared input and output directories")
                if journal:
                    admit_tools()
                    remote = remote_name(name)
                    if name in {"Read", "Glob", "Grep", "Write", "Edit", "Bash"} or remote:
                        key = data.get("tool_use_id") or call_id
                        operation = begin_tools([{"id": key, "name": name, "arguments": fields,
                                                                   "read_only": name in {"Read", "Glob", "Grep"},
                                                                   "pre_effect_error_codes": ["permission_denied"]}])
                        operations[key] = operation
                        if remote:
                            remote_calls[key] = (*remote, fields)
                            remote_sessions[key] = data.get("session_id")
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                if interaction:
                    await interaction.tool(data.get("tool_use_id") or call_id, name, fields, "failed", str(error))
                return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                               "permissionDecisionReason": str(error)}}
            key = data.get("tool_use_id") or call_id
            if key:
                tool_spans[key] = trace.get_tracer("agentteams.claude-code").start_span("claude-code.tool",
                    attributes={**identity_attributes(), "langfuse.observation.type":"tool", "gen_ai.tool.name": name, "gen_ai.tool.call.id": key,
                                "agentteams.session.id": data.get("session_id") or ""})
                set_io(tool_spans[key], input=fields)
                if interaction:
                    await interaction.tool(key, name, fields, "running")
            return {}

        async def after_tool(data, call_id, _context):
            nonlocal confirmed
            key = data.get("tool_use_id") or call_id
            response = data.get("tool_response")
            success = data.get("hook_event_name") == "PostToolUse" and not (
                isinstance(response, dict) and (response.get("isError") is True or response.get("is_error") is True))
            if key in remote_returns:
                response = remote_returns[key]
                success = not (response.get('httpStatus', 200) >= 400 or 'error' in response
                               or (response.get('result') or {}).get('isError') is True)
            tool_span = tool_spans.pop(key, None)
            if tool_span:
                set_io(tool_span, output=response if key in remote_returns or success else {"error": data.get("error"), "response": response})
                if not success:
                    tool_span.set_status(trace.StatusCode.ERROR)
                tool_span.end()
            if success and data["tool_name"] == "mcp__teamharness__task":
                completed = task_completion(response)
                if completed:
                    task["_completed"] = dict(completed, callId=key, sessionId=data.get("session_id"))
                    if interaction:
                        await interaction.tool(key, data["tool_name"], data.get("tool_input") or {}, "succeeded", response)
                    return {"continue_": False, "stopReason": "Task phase completed"}
            if key in operations:
                name = data["tool_name"]
                failed_local = (name in {"Bash", "Write", "Edit"} and data.get("hook_event_name") == "PostToolUseFailure"
                                and data.get("is_interrupt") is False)
                incomplete_bash = name == "Bash" and not failed_local and (not isinstance(response, dict)
                    or response.get("interrupted") is not False or response.get("backgroundTaskId")
                    or response.get("background_task_id"))
                if incomplete_bash or (not success and name not in {"Read", "Glob", "Grep"} and not failed_local):
                    if interaction:
                        await interaction.tool(key, name, data.get("tool_input") or {}, "failed",
                            data.get("error") or "命令尚无可核验的结束结果。")
                    task["_unverified_tool"] = name
                    await client.interrupt()
                    return {"continue_": False, "stopReason": "Task tool effect requires verification"}
                artifacts = []
                files = []
                if success and name in {"Write", "Edit"} and not matrix:
                    artifact, staged = stage_output(identity, workspace, data["tool_input"]["file_path"], operations[key])
                    artifacts.append(artifact)
                    files.append(staged)
                result = {"id": key, "content": artifacts or {"response_sha256": Recovery.digest(data.get("tool_response") if success else data.get("error"))}}
                if matrix:
                    result['content'] = {'input': data.get('tool_input'), 'response': response if success else data.get('error')}
                if not success:
                    result["status"] = "error"
                    if name in {"Read", "Glob", "Grep"}:
                        result["effect"] = "none"
                if not matrix:
                    persist_terminal(identity, operations[key], result, artifacts, files=files)
                saved = finish_tool(operations[key], result, artifacts)
                if data.get("session_id"):
                    confirmed = (data["session_id"], key, saved["execution"])
                del operations[key]
            if interaction:
                await interaction.tool(key, data["tool_name"], data.get("tool_input") or {},
                    "succeeded" if success else "failed", response if success else data.get("error") or response)
            return {}

        async def permission(name, fields, context):
            from runtime_control import mcp_approval
            server = next((key for key in servers if key != "teamharness" and name.startswith("mcp__" + key + "__")), None)
            policy = mcp_approval(record, server) if server else mode
            if name != "AskUserQuestion" and policy == "allow":
                return PermissionResultAllow(updated_input=fields)
            result = False if interaction is None else await interaction.permission(name, fields, context.tool_use_id)
            if result is False:
                key = context.tool_use_id
                if key in operations:
                    # This callback is denying execution, so no native command
                    # has run. A rejection/expiry is a terminal no-effect fact,
                    # not an interrupted shell whose effects are unknown.
                    receipt = {"id": key, "status": "error", "effect": "none", "error_code": "permission_denied",
                               "content": {"permission_decision": "deny"}}
                    if not matrix:
                        persist_terminal(identity, operations[key], receipt, [])
                    finish_tool(operations[key], receipt)
                    del operations[key]
                tool_span = tool_spans.pop(key, None)
                if tool_span:
                    tool_span.set_status(trace.StatusCode.ERROR)
                    tool_span.end()
                return PermissionResultDeny(message="Human approval was unavailable, rejected, or expired")
            return PermissionResultAllow(updated_input=result if isinstance(result, dict) else fields)

        allowed_mcp = ["mcp__teamharness__task"] if decision or task.get("execution") else ["mcp__teamharness__*"]
        options = ClaudeAgentOptions(
            model=model, cwd=str(workspace), resume=session, session_id=new_session_id, tools=tools,
            allowed_tools=[name for name in tools if name != "AskUserQuestion"
                           and (name != "Bash" or os.getenv("AGENTTEAMS_YOLO") == "1")] + allowed_mcp,
            permission_mode="default", can_use_tool=permission, include_partial_messages=True,
            system_prompt={"type": "preset", "preset": "claude_code", "append": instructions, "snapshot": False},
            setting_sources=[], plugins=[{"type": "local", "path": directory}],
            mcp_servers=servers, strict_mcp_config=True,
            add_dirs=[str(path) for path in input_directories(context)],
            hooks={"PreToolUse": [HookMatcher(hooks=[before_tool])],
                   "PostToolUse": [HookMatcher(hooks=[after_tool])],
                   "PostToolUseFailure": [HookMatcher(hooks=[after_tool])]},
            env={"ANTHROPIC_BASE_URL": provider["baseUrl"].rstrip("/").removesuffix("/v1"),
                 "ANTHROPIC_AUTH_TOKEN": provider["apiKey"], "ANTHROPIC_API_KEY": "",
                 "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                 **({"CLAUDE_CODE_DISABLE_BACKGROUND_TASKS": "1"} if identity else {})},
        )
        try:
            async with model_endpoint(provider) as base_url, observed_servers(servers, reserve_remote, returned_remote) as observed:
                options.env["ANTHROPIC_BASE_URL"] = base_url
                options.mcp_servers = observed
                async with ClaudeSDKClient(options=options) as client:
                    watcher = None
                    if interaction:
                        await interaction.start()
                        watcher = asyncio.create_task(interaction.watch(client))
                    try:
                        return await receive(client, interaction, watcher, span, refreshed, prompt, task)
                    finally:
                        if watcher:
                            watcher.cancel()
                            with suppress(asyncio.CancelledError):
                                await watcher
                        if interaction:
                            await interaction.close("cancelled" if interaction.stopped else "expired")
        finally:
            # Client teardown flushes the native tool_result before publishing
            # its cache descriptor, including when a later model request fails.
            remember_confirmed()
            if interaction:
                await interaction.close("cancelled" if interaction.stopped else "expired", terminal=True)
                if interaction.stopped:
                    # Publish stopped only after the SDK process and its tools exit.
                    await interaction.status("stopped")


async def receive(client, interaction, watcher, span, refreshed, prompt, task):
    try:
        if any(task.get(key) for key in ("review", "planning", "completion", "execution")) or (task.get("sources") and os.getenv("AGENTTEAMS_CONTROLLER_URL")):
            with anyio.fail_after(30):
                while True:
                    status = await client.get_mcp_status()
                    server = next((item for item in status["mcpServers"] if item["name"] == "teamharness"), {})
                    tool = next((t for t in server.get("tools", []) if t["name"] == "task"), None)
                    if server.get("status") == "connected" and tool:
                        task_name = f"mcp__{server['name']}__{tool['name']}"
                        break
                    if server.get("status") in {"failed", "disabled", "needs-auth"}:
                        raise RuntimeError("The registered TeamHarness Task tool is unavailable")
                    await asyncio.sleep(0.1)
            prompt = f"Use the registered Task tool {task_name} for Task operations.\n" + prompt
        await client.query(prompt)
        stream = client.receive_response()
        while True:
            # The CLI owns API timeouts. Keep controls responsive while it is
            # thinking or waiting for an upstream response without SDK output.
            reader = asyncio.create_task(anext(stream))
            try:
                while not reader.done():
                    done, _ = await asyncio.wait([reader, *([watcher] if watcher else [])], timeout=0.5,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    if watcher and watcher in done:
                        await watcher  # Surface failures instead of silently disabling human control.
                        if interaction.stopped:
                            raise TurnStopped(interaction.session)
                    if interaction and interaction.stopped and interaction.stop_started and time.monotonic() - interaction.stop_started > 5:
                        raise TurnStopped(interaction.session)
                try:
                    message = reader.result()
                except StopAsyncIteration:
                    break
            finally:
                if not reader.done():
                    reader.cancel()
                    with suppress(asyncio.CancelledError):
                        await reader
            if isinstance(message, SystemMessage) and message.subtype == "init":
                span.set_attribute("agentteams.session.id", message.data.get("session_id") or "")
                span.set_attribute("langfuse.session.id", message.data.get("session_id") or "")
                span.set_attribute("agentteams.turn.id", message.data.get("uuid") or "")
                if interaction:
                    interaction.session = message.data.get("session_id")
            if isinstance(message, StreamEvent) and interaction:
                await interaction.stream(message.event)
            if isinstance(message, AssistantMessage):
                with trace.get_tracer("agentteams.claude-code").start_as_current_span("claude-code.model.response", attributes=identity_attributes()) as result_span:
                    result_span.set_attribute("gen_ai.response.model", message.model)
                    result_span.set_attribute("gen_ai.response.id", getattr(message, "message_id", None) or "")
            if isinstance(message, ResultMessage):
                if task.get("_unverified_tool"):
                    raise UnverifiedToolEffect(f"{task['_unverified_tool']} 尚未确认执行结果，需要核对后继续。")
                if interaction and not interaction.confirm_inputs(message):
                    # A result for the original input can race with a steer.
                    # Keep the native session open until its UUID is consumed.
                    await stream.aclose()
                    stream = client.receive_response()
                    continue
                completed = task.get("_completed")
                if completed:
                    if not persisted_task_receipt(message.session_id, completed):
                        raise RuntimeError("Task receipt is missing from the native session")
                    if interaction:
                        await interaction.status("completed")
                    return message.session_id, completed.get("displayText") or completed["fact"]
                if interaction and interaction.stopped:
                    raise TurnStopped(message.session_id)
                if message.is_error or message.subtype != "success" or not message.result:
                    reason = message.terminal_reason or message.subtype
                    if message.api_error_status is not None:
                        reason = f"gateway HTTP {message.api_error_status}"
                    raise RuntimeError(f"Claude Code returned no usable result ({reason})")
                if refreshed.is_file():
                    task["refresh"] = json.loads(refreshed.read_text())
                if interaction:
                    await interaction.status("completed")
                return message.session_id, message.result
        raise RuntimeError("Claude Code exited without a result")
    except TurnStopped:
        raise
    except BaseException as error:
        if interaction:
            title = str(error) if isinstance(error, UnverifiedToolEffect) else "Claude execution failed"
            await interaction.status("failed")
            await interaction.block("ending", {"type": "error", "kind": "failed", "title": title}, title)
        raise


def task_completion(response):
    if isinstance(response, list):
        response = {"content": response}
    if not isinstance(response, dict) or response.get("isError") or response.get("is_error"):
        return None
    for part in response.get("content") or []:
        if not isinstance(part, dict) or not isinstance(part.get("text"), str):
            continue
        try:
            value, _ = json.JSONDecoder().raw_decode(part["text"].lstrip())
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("phaseComplete") is True and value.get("receiptId") and value.get("fact"):
            return value
    return None


def persisted_task_receipt(session, completed):
    if session != completed.get("sessionId"):
        return False
    projects = Path(os.getenv("CLAUDE_CONFIG_DIR") or Path.home() / ".claude") / "projects"
    for path in projects.glob("*/" + session + ".jsonl"):
        for line in path.read_text().splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            content = (row.get("message") or {}).get("content")
            if row.get("type") != "user" or not isinstance(content, list):
                continue
            for block in content:
                if block.get("type") == "tool_result" and block.get("tool_use_id") == completed["callId"]:
                    payload = block.get("content")
                    if isinstance(payload, str):
                        payload = [{"text": payload}]
                    found = task_completion({"content": payload})
                    if found and found["receiptId"] == completed["receiptId"]:
                        return True
    return False
