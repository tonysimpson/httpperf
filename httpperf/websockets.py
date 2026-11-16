import asyncio
import os
import ssl as _ssl
import struct
import time
from urllib.parse import urlparse

import httpperf
from httpperf import _httpperf

__all__ = [
    "ClientConnection",
    "Close",
    "ConnectionClosed",
    "ConnectionClosedError",
    "ConnectionClosedOK",
    "InvalidHandshake",
    "InvalidStatus",
    "InvalidURI",
    "WebSocketException",
    "connect",
]

TEXT, BINARY, CONT, CLOSE, PING, PONG = 1, 2, 4, 8, 16, 64
FINAL = 0x80000000
OK_CLOSE_CODES = {1000, 1001, 1005}


class WebSocketException(Exception):
    pass


class Close:
    __slots__ = ("code", "reason")

    def __init__(self, code, reason):
        self.code = code
        self.reason = reason

    def __repr__(self):
        return "Close(code=%d, reason=%r)" % (self.code, self.reason)


class ConnectionClosed(WebSocketException):
    def __init__(self, rcvd, sent, rcvd_then_sent=None):
        self.rcvd = rcvd
        self.sent = sent
        self.rcvd_then_sent = rcvd_then_sent
        super().__init__(self._describe())

    def _describe(self):
        if self.rcvd is None and self.sent is None:
            return "no close frame received or sent"
        if self.rcvd is None:
            return "sent %d (%s); no close frame received" % (self.sent.code, self.sent.reason)
        if self.sent is None:
            return "received %d (%s); no close frame sent" % (self.rcvd.code, self.rcvd.reason)
        return "received %d (%s); then sent %d (%s)" % (
            self.rcvd.code,
            self.rcvd.reason,
            self.sent.code,
            self.sent.reason,
        )

    @property
    def code(self):
        return 1006 if self.rcvd is None else self.rcvd.code

    @property
    def reason(self):
        return "" if self.rcvd is None else self.rcvd.reason


class ConnectionClosedOK(ConnectionClosed):
    pass


class ConnectionClosedError(ConnectionClosed):
    pass


class InvalidHandshake(WebSocketException):
    pass


class InvalidStatus(InvalidHandshake):
    def __init__(self, response):
        self.response = response
        super().__init__("server rejected WebSocket connection: HTTP %d" % response.status_code)


class InvalidURI(WebSocketException):
    def __init__(self, uri, msg):
        self.uri = uri
        self.msg = msg
        super().__init__("%s isn't a valid URI: %s" % (uri, msg))


def _closed(rcvd, sent):
    ok = rcvd is not None and rcvd.code in OK_CLOSE_CODES and sent is not None and sent.code in OK_CLOSE_CODES
    return (ConnectionClosedOK if ok else ConnectionClosedError)(rcvd, sent, rcvd is not None)


def _close_payload(code, reason):
    return b"" if code == 1005 else struct.pack("!H", code) + reason.encode()


def _header_lines(headers):
    if headers is None:
        return []
    items = headers.raw_items() if hasattr(headers, "raw_items") else getattr(headers, "items", lambda: headers)()
    return ["%s: %s" % (name, value) for name, value in items]


def _verify(ssl, verify):
    if ssl is None or ssl is True:
        return verify
    if ssl.verify_mode == _ssl.CERT_NONE:
        return False
    certs = ssl.get_ca_certs(binary_form=True)
    if not certs:
        return True if verify is None else verify
    return "".join(_ssl.DER_cert_to_PEM_cert(cert) for cert in certs).encode()


class connect:
    def __init__(
        self,
        uri,
        *,
        additional_headers=None,
        subprotocols=None,
        origin=None,
        user_agent_header=None,
        ssl=None,
        verify=None,
        open_timeout=10,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=2**20,
        max_queue=16,
        compression="deflate",
        session=None,
        logger=None,
    ):
        self._uri = uri
        self._headers = _header_lines(additional_headers)
        if subprotocols:
            self._headers.append("Sec-WebSocket-Protocol: " + ", ".join(subprotocols))
        if origin is not None:
            self._headers.append("Origin: " + origin)
        if user_agent_header is not None:
            self._headers.append("User-Agent: " + user_agent_header)
        self._verify = _verify(ssl, verify)
        self._open_timeout = open_timeout
        self._options = (ping_interval, ping_timeout, close_timeout, max_size, max_queue)
        self._session = session
        self._connection = None

    def __await__(self):
        return self._open().__await__()

    async def __aenter__(self):
        self._connection = await self._open()
        return self._connection

    async def __aexit__(self, *exc_info):
        await self._connection.close()

    async def _open(self):
        parts = urlparse(self._uri)
        if parts.scheme not in ("ws", "wss") or not parts.hostname:
            raise InvalidURI(self._uri, "scheme isn't ws or wss" if parts.hostname else "no host")
        if not _httpperf.websockets:
            raise RuntimeError("WebSockets need libcurl 8.16 or later, built with WebSocket support")
        own_session = self._session is None
        session = httpperf.Session() if own_session else self._session
        try:
            connection = await asyncio.wait_for(self._handshake(session), self._open_timeout)
        except BaseException:
            if own_session:
                session.close()
            raise
        connection._own_session = session if own_session else None
        return connection

    async def _handshake(self, session):
        loop = asyncio.get_running_loop()
        options = dict(session._options, timeout=0, stream=True, upload_size=-1, websocket=True)
        if self._verify is not None:
            options["verify"] = httpperf._verify(self._verify)
        headers = tuple(self._headers) or None
        future = session._submit(loop, "GET", self._uri, headers=headers, cookies=None, auth=None, data=None, **options)
        future._handle, upload = future._handle
        resp = await future
        start = time.time()
        request = httpperf.Request("GET", self._uri, headers, None, None, None)
        response = httpperf.Response(request, resp, start, start, session._json_loads, True)
        if response.status_code != 101:
            response.close()
            raise InvalidStatus(response)
        return ClientConnection(response, upload, *self._options)


class ClientConnection:
    def __init__(self, response, upload, ping_interval, ping_timeout, close_timeout, max_size, max_queue):
        loop = asyncio.get_running_loop()
        self.response = response
        self.subprotocol = response.headers.get("Sec-WebSocket-Protocol")
        self.latency = 0.0
        self._upload = upload
        self._close_timeout = close_timeout
        self._max_size = max_size
        self._messages = asyncio.Queue(max_queue or 0)
        self._send_lock = asyncio.Lock()
        self._pings = {}
        self._pongs = set()
        self._linger = None
        self._rcvd = None
        self._sent = None
        self._closed = loop.create_future()
        self._own_session = None
        self._reader = loop.create_task(self._read())
        self._keepalive = None
        if ping_interval is not None:
            self._keepalive = loop.create_task(self._keep_alive(ping_interval, ping_timeout))

    @property
    def close_code(self):
        if not self._closed.done():
            return None
        return 1006 if self._rcvd is None else self._rcvd.code

    @property
    def close_reason(self):
        if not self._closed.done():
            return None
        return "" if self._rcvd is None else self._rcvd.reason

    async def recv(self):
        if self._messages.empty() and self._closed.done():
            raise _closed(self._rcvd, self._sent)
        message = await self._messages.get()
        if message is None:
            self._messages.put_nowait(None)
            raise _closed(self._rcvd, self._sent)
        return message

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        while True:
            try:
                yield await self.recv()
            except ConnectionClosedOK:
                return

    async def send(self, message):
        if isinstance(message, str):
            await self._send_frame(TEXT, message.encode())
        elif isinstance(message, (bytes, bytearray, memoryview)):
            await self._send_frame(BINARY, bytes(message))
        elif hasattr(message, "__aiter__") or hasattr(message, "__iter__"):
            await self._send_fragments(message)
        else:
            raise TypeError("message must be str, bytes or an iterable of them")

    async def _send_fragments(self, fragments):
        kind = None
        previous = None
        async for fragment in _each(fragments):
            frame_kind = TEXT if isinstance(fragment, str) else BINARY
            if kind is not None and frame_kind != kind:
                raise TypeError("all fragments must be str, or all bytes")
            kind = frame_kind
            if previous is not None:
                await self._send_frame(kind | CONT, previous)
            previous = fragment.encode() if isinstance(fragment, str) else bytes(fragment)
        if previous is not None:
            await self._send_frame(kind, previous)

    async def ping(self, data=None):
        if data is None:
            data = os.urandom(4)
            while data in self._pings:
                data = os.urandom(4)
        elif isinstance(data, str):
            data = data.encode()
        waiter = asyncio.get_running_loop().create_future()
        self._pings[bytes(data)] = (waiter, time.monotonic())
        await self._send_frame(PING, bytes(data))
        return waiter

    async def pong(self, data=b""):
        await self._send_frame(PONG, data.encode() if isinstance(data, str) else bytes(data))

    async def close(self, code=1000, reason=""):
        if not self._closed.done() and self._sent is None:
            try:
                await self._send_close(code, reason)
            except ConnectionClosed:
                pass
        try:
            await asyncio.wait_for(asyncio.shield(self._closed), self._close_timeout)
        except asyncio.TimeoutError:
            self.response.close()
            await asyncio.shield(self._closed)

    async def wait_closed(self):
        await asyncio.shield(self._closed)

    async def _send_close(self, code, reason):
        self._sent = Close(code, reason)
        await self._send_frame(CLOSE, _close_payload(code, reason), closing=True)

    async def _send_frame(self, flags, payload, closing=False):
        async with self._send_lock:
            if self._closed.done() or (self._sent is not None and not closing):
                raise _closed(self._rcvd, self._sent)
            loop = asyncio.get_running_loop()
            view = memoryview(struct.pack("<IQ", flags, len(payload)) + payload)
            while view:
                n = _httpperf.upload_push(self._upload, view)
                if n is None:
                    raise _closed(self._rcvd, self._sent)
                view = view[n:]
                if view:
                    waiter = loop.create_future()
                    if _httpperf.upload_wait(self._upload, waiter):
                        await waiter

    async def _read(self):
        try:
            await self._read_frames()
        except httpperf.RequestError:
            pass
        finally:
            self._finish()

    async def _read_frames(self):
        buffer = bytearray()
        parts = []
        size = 0
        async for data in self.response._chunks():
            buffer += data
            offset = 0
            while len(buffer) - offset >= 8:
                flags, length = struct.unpack_from("<II", buffer, offset)
                if len(buffer) - offset - 8 < length:
                    break
                chunk = bytes(buffer[offset + 8 : offset + 8 + length])
                offset += 8 + length
                if flags & (TEXT | BINARY):
                    parts.append(chunk)
                    size += length
                    if self._max_size is not None and size > self._max_size:
                        await self._fail(1009, "message too big")
                        return
                    if flags & FINAL and not flags & CONT:
                        message = b"".join(parts)
                        parts, size = [], 0
                        if flags & TEXT:
                            try:
                                message = message.decode()
                            except UnicodeDecodeError:
                                await self._fail(1007, "invalid UTF-8")
                                return
                        await self._messages.put(message)
                elif flags & CLOSE:
                    parts.append(chunk)
                    if flags & FINAL:
                        await self._received_close(b"".join(parts))
                        parts = []
                elif flags & PING and flags & FINAL:
                    task = asyncio.get_running_loop().create_task(self._auto_pong(chunk))
                    self._pongs.add(task)
                    task.add_done_callback(self._pongs.discard)
                elif flags & PONG and flags & FINAL:
                    self._received_pong(chunk)
            del buffer[:offset]

    async def _received_close(self, payload):
        if len(payload) >= 2:
            code = struct.unpack("!H", payload[:2])[0]
            reason = payload[2:].decode(errors="replace")
        else:
            code, reason = 1005, ""
        self._rcvd = Close(code, reason)
        if self._sent is None:
            try:
                await self._send_close(code, reason)
            except ConnectionClosed:
                pass
        if self._close_timeout is not None:
            self._linger = asyncio.get_running_loop().call_later(self._close_timeout, self.response.close)

    async def _auto_pong(self, data):
        try:
            await self._send_frame(PONG, data)
        except ConnectionClosed:
            pass

    def _received_pong(self, data):
        if data not in self._pings:
            return
        now = time.monotonic()
        for key in list(self._pings):
            waiter, sent = self._pings.pop(key)
            self.latency = now - sent
            if not waiter.done():
                waiter.set_result(self.latency)
            if key == data:
                break

    async def _fail(self, code, reason):
        if self._sent is None:
            try:
                await self._send_close(code, reason)
            except ConnectionClosed:
                pass
        self.response.close()

    async def _keep_alive(self, interval, timeout):
        try:
            while True:
                await asyncio.sleep(interval)
                waiter = await self.ping()
                if timeout is None:
                    continue
                try:
                    await asyncio.wait_for(asyncio.shield(waiter), timeout)
                except asyncio.TimeoutError:
                    await self._fail(1011, "keepalive ping timeout")
                    return
        except ConnectionClosed:
            pass

    def _finish(self):
        if self._closed.done():
            return
        self._closed.set_result(None)
        self.response.close()
        if self._linger is not None:
            self._linger.cancel()
        if self._keepalive is not None and self._keepalive is not asyncio.current_task():
            self._keepalive.cancel()
        error = _closed(self._rcvd, self._sent)
        for waiter, _ in self._pings.values():
            if not waiter.done():
                waiter.set_exception(error)
                waiter.exception()
        self._pings.clear()
        try:
            self._messages.put_nowait(None)
        except asyncio.QueueFull:
            self._messages._queue.append(None)
        if self._own_session is not None:
            self._own_session.close()


async def _each(items):
    if hasattr(items, "__aiter__"):
        async for item in items:
            yield item
    else:
        for item in items:
            yield item
