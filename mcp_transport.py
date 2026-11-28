"""Observe native HTTP MCP returns before the CLI flattens their error types.

The CLI still owns MCP discovery, sessions, capabilities and approvals. This
loopback hop forwards the protocol and reports only matching tool returns.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import re
import socket
from urllib.parse import urljoin, urlsplit, urlunsplit
from uuid import uuid4


_HOP_HEADERS = {'host', 'connection', 'keep-alive', 'transfer-encoding', 'te',
                'trailer', 'upgrade', 'proxy-authenticate', 'proxy-authorization'}


@asynccontextmanager
async def observed_servers(servers, reserve, returned):
    from contextlib import AsyncExitStack
    import httpx
    import uvicorn
    from starlette.applications import Starlette
    from starlette.responses import Response, StreamingResponse
    from starlette.routing import Route

    targets = {uuid4().hex: (name, config['url']) for name, config in servers.items()
               if config.get('type') == 'http'}
    if not targets:
        yield servers
        return
    pending = {}
    sock = socket.create_server(('127.0.0.1', 0))
    address = 'http://127.0.0.1:' + str(sock.getsockname()[1])

    async def observe(name, session, message):
        if not isinstance(message, dict) or 'id' not in message:
            return
        if 'result' not in message and 'error' not in message:
            return
        binding = pending.pop((name, session, json.dumps(message['id'])), None)
        if binding is not None:
            await returned(binding, message)

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(timeout=None, follow_redirects=False))

        async def proxy(request):
            target = targets.get(request.path_params['target'])
            if target is None:
                return Response(status_code=404)
            name, url = target
            if request.url.query:
                parts = urlsplit(url)
                query = '&'.join(value for value in (parts.query, request.url.query) if value)
                url = urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
            body = await request.body()
            session = request.headers.get('mcp-session-id', '')
            binding = None
            try:
                message = json.loads(body)
            except ValueError:
                message = None
            if isinstance(message, dict) and message.get('method') == 'tools/call':
                params = message.get('params') or {}
                binding = reserve(name, params.get('name'), params.get('arguments') or {})
                if binding is not None:
                    pending[name, session, json.dumps(message['id'])] = binding
            headers = {key: value for key, value in request.headers.items() if key.lower() not in _HOP_HEADERS}
            outgoing = client.build_request(request.method, url, content=body, headers=headers)
            try:
                response = await client.send(outgoing, stream=True)
            except httpx.TransportError as error:
                # Keep this unconfirmed: a local failure is not an upstream return.
                # Exception text can contain credentials from a URL; retain its
                # typed cause without copying that text into the native result.
                return Response('MCP upstream transport failed (' + type(error).__name__
                                + '); operation outcome is unconfirmed',
                                status_code=502, media_type='text/plain')
            response_headers = {key: value for key, value in response.headers.items()
                                if key.lower() not in _HOP_HEADERS | {'content-length', 'content-encoding'}}
            if location := response_headers.get('location'):
                location = urljoin(url, location)
                if urlsplit(location).netloc == urlsplit(url).netloc and urlsplit(location).scheme == urlsplit(url).scheme:
                    token = uuid4().hex
                    targets[token] = (name, location)
                    response_headers['location'] = address + '/' + token
                else:
                    response_headers['location'] = location
            sse = response.headers.get('content-type', '').split(';')[0].strip() == 'text/event-stream'

            async def content():
                buffer, data = b'', []
                try:
                    async for chunk in response.aiter_bytes():
                        buffer += chunk
                        if sse:
                            while separator := re.search(rb'\r\n|\r(?!$)|\n', buffer):
                                line, buffer = buffer[:separator.start()], buffer[separator.end():]
                                if not line and data:
                                    try:
                                        await observe(name, session, json.loads(b'\n'.join(data)))
                                    except ValueError:
                                        pass
                                    data = []
                                elif line.startswith(b'data:'):
                                    data.append(line[5:].removeprefix(b' '))
                        yield chunk
                    if not sse and buffer:
                        if response.status_code >= 400 and binding is not None:
                            pending.pop((name, session, json.dumps(message['id'])), None)
                            await returned(binding, {'httpStatus': response.status_code,
                                                     'body': buffer.decode('utf-8', 'replace')})
                        else:
                            try:
                                await observe(name, session, json.loads(buffer))
                            except ValueError:
                                pass
                    elif response.status_code >= 400 and binding is not None:
                        pending.pop((name, session, json.dumps(message['id'])), None)
                        await returned(binding, {'httpStatus': response.status_code})
                finally:
                    await response.aclose()

            return StreamingResponse(content(), status_code=response.status_code, headers=response_headers)

        app = Starlette(routes=[Route('/{target}', proxy, methods=['GET', 'POST', 'DELETE'])])
        server = uvicorn.Server(uvicorn.Config(app, log_config=None, access_log=False, lifespan='off',
                                             timeout_graceful_shutdown=2))
        running = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            while not server.started:
                if running.done():
                    await running
                    raise RuntimeError('Claude MCP observer did not start')
                await asyncio.sleep(0.01)
            urls = {name: address + '/' + token for token, (name, _) in targets.items()}
            yield {name: {**config, 'url': urls[name]} if name in urls else config
                   for name, config in servers.items()}
        finally:
            server.should_exit = True
            await running
            sock.close()
