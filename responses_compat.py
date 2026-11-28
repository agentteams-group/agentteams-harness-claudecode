"""Keep the Claude SDK native while translating a declared Responses upstream."""
import asyncio
from contextlib import asynccontextmanager
from contextlib import suppress
import hmac
import json
import os
import socket
import threading
import time


# Transport controls are supplied by the Harness, never by the model request.
_MESSAGE_FIELDS = {
    "model", "messages", "max_tokens", "system", "metadata", "stop_sequences",
    "stream", "temperature", "top_p", "top_k", "tools", "tool_choice",
    "thinking", "output_config", "output_format", "context_management",
    "service_tier", "container",
}
_PING_SECONDS = 15


async def convert_messages(body, provider):
    # Conversion needs the bundled schemas, not a remote pricing-map refresh.
    os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    import litellm
    from litellm import anthropic
    from litellm.exceptions import ModelNotMappedError
    from responses_events import ResponsesReasoningEvents
    if not any(isinstance(callback, ResponsesReasoningEvents) for callback in litellm.callbacks):
        litellm.callbacks.append(ResponsesReasoningEvents())
    try:
        litellm.get_model_info(body["model"], custom_llm_provider="openai")
    except ModelNotMappedError:
        # A saved Responses gateway model is not necessarily in the public
        # pricing map. Unknown must not silently turn streaming into buffering.
        litellm.register_model({"openai/" + body["model"]: {
            "litellm_provider": "openai", "mode": "responses",
            "supports_native_streaming": True, "supports_reasoning": True,
        }})
    # The CLI can include mid-turn system updates. Preserve every instruction
    # in order while emitting the one leading system message required by the
    # Chat Completions upstream behind a Responses gateway.
    instructions = [body.get("system", "")]
    messages = []
    for message in body["messages"]:
        if message["role"] == "system":
            instructions.append(message.get("content", ""))
        else:
            messages.append(message)
    text = []
    for content in instructions:
        text.extend([content] if isinstance(content, str) else [block["text"] for block in content])
    body = {**body, "system": "\n\n".join(part for part in text if part), "messages": messages}
    payload = {**body, "model": "openai/" + body["model"],
               "api_base": provider["baseUrl"].rstrip("/"), "api_key": provider["apiKey"]}
    return await anthropic.messages.acreate(**payload)


def _json(value):
    return value.model_dump(exclude_none=True) if hasattr(value, "model_dump") else value


async def stream_events(body, provider):
    result = None
    pending = asyncio.create_task(convert_messages(body, provider))
    try:
        while True:
            ready, _ = await asyncio.wait({pending}, timeout=_PING_SECONDS)
            if not ready:
                # Native Anthropic ping frames carry no model content. Keep
                # the CLI connection alive while the upstream is reasoning.
                yield 'event: ping\ndata: {"type":"ping"}\n\n'
                continue
            value = pending.result()
            if result is None:
                result = value.__aiter__()
            elif isinstance(value, (bytes, str)):
                yield value
            else:
                event = _json(value)
                yield "event: " + event["type"] + "\ndata: " + json.dumps(event, ensure_ascii=False) + "\n\n"
            pending = asyncio.create_task(anext(result))
    except StopAsyncIteration:
        pass
    except Exception as exc:
        message = str(exc).replace(provider["apiKey"], "[redacted]")
        kind = {400: "invalid_request_error", 401: "authentication_error", 403: "permission_error",
                404: "not_found_error", 413: "request_too_large", 429: "rate_limit_error",
                529: "overloaded_error"}.get(getattr(exc, "status_code", None), "api_error")
        yield 'event: error\ndata: ' + json.dumps({"type": "error", "error": {
            "type": kind, "message": message}}, ensure_ascii=False) + '\n\n'
    finally:
        if not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        close = getattr(result, "aclose", None)
        if close:
            await close()


def responses_app(provider):
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, StreamingResponse
    from starlette.routing import Route

    def error(message, status):
        return JSONResponse({"type":"error", "error":{"type":"invalid_request_error", "message":message}}, status_code=status)

    async def messages(request):
        credential = request.headers.get("authorization", "").removeprefix("Bearer ") or request.headers.get("x-api-key", "")
        if not hmac.compare_digest(credential, provider["apiKey"]):
            return error("Invalid local SDK credential", 401)
        try:
            body = await request.json()
        except ValueError:
            return error("Expected an Anthropic Messages JSON object", 400)
        if not isinstance(body, dict) or body.get("model") != provider["models"][0]["id"]:
            return error("The model must match the saved Agent configuration", 400)
        if body.keys() - _MESSAGE_FIELDS:
            return error("Unsupported Anthropic request fields", 400)
        # LiteLLM otherwise falls back to Chat Completions, which is not this mode.
        if body.get("stop_sequences"):
            return error("stop_sequences is not supported by the Responses upstream", 400)
        if body.get("stream"):
            return StreamingResponse(stream_events(body, provider), media_type="text/event-stream")
        try:
            result = await convert_messages(body, provider)
        except Exception as exc:
            return error(str(exc).replace(provider["apiKey"], "[redacted]"), getattr(exc, "status_code", 502) or 502)
        return JSONResponse(_json(result))

    return Starlette(routes=[Route("/v1/messages", messages, methods=["POST"])])


@asynccontextmanager
async def model_endpoint(provider):
    mode = os.getenv("AGENTTEAMS_CLAUDE_UPSTREAM_API", "anthropic")
    if mode == "anthropic":
        yield provider["baseUrl"].rstrip("/").removesuffix("/v1")
        return
    if mode != "responses":
        raise ValueError("AGENTTEAMS_CLAUDE_UPSTREAM_API must be anthropic or responses")
    import uvicorn
    sock = socket.create_server(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(responses_app(provider), log_config=None, access_log=False,
                                         lifespan="off", timeout_graceful_shutdown=2))
    thread = threading.Thread(target=server.run, kwargs={"sockets":[sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if not thread.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError("Claude Responses adapter did not start")
            await asyncio.sleep(0.01)
        yield "http://127.0.0.1:" + str(sock.getsockname()[1])
    finally:
        server.should_exit = True
        await asyncio.to_thread(thread.join, 5)
        sock.close()
