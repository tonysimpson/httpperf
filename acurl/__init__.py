import asyncio
import collections.abc
import datetime
import http
import json as _json
import os
import threading
import time
from urllib.parse import urlencode, urlparse

from acurl import _acurl

_json_encoder = _json.JSONEncoder(separators=(",", ":"), check_circular=False)


class RequestError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class Timeout(RequestError):
    pass


class HTTPError(RequestError):
    def __init__(self, message, response):
        super().__init__(message)
        self.response = response


CURLE_OPERATION_TIMEDOUT = 28


_FALSE_TRUE = ["FALSE", "TRUE"]


class Cookie:
    __slots__ = "_http_only _domain _include_subdomains _path _is_secure _expiration _name _value".split()

    def __init__(self, http_only, domain, include_subdomains, path, is_secure, expiration, name, value):
        self._http_only = http_only
        self._domain = domain
        self._include_subdomains = include_subdomains
        self._path = path
        self._is_secure = is_secure
        self._expiration = expiration
        self._name = name
        self._value = value

    def __repr__(self):
        return (
            "Cookie(http_only={}, domain={}, include_subdomains={}, path={}, is_secure={}, "
            "expiration={}, name={}, value={})"
        ).format(
            self._http_only,
            self._domain,
            self._include_subdomains,
            self._path,
            self._is_secure,
            self._expiration,
            self._name,
            self._value,
        )

    def __str__(self):
        return self.__repr__()

    @property
    def http_only(self):
        return self._http_only

    @property
    def domain(self):
        return self._domain

    @property
    def include_subdomains(self):
        return self._include_subdomains

    @property
    def path(self):
        return self._path

    @property
    def is_secure(self):
        return self._is_secure

    @property
    def expiration(self):
        return self._expiration

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def has_expired(self):
        return self.expiration != 0 and time.time() > self.expiration

    def format(self):
        bits = []
        if self.http_only:
            bits.append("#HttpOnly_")
        bits.append(self.domain)
        bits.append("\t")
        bits.append(_FALSE_TRUE[self.include_subdomains])
        bits.append("\t")
        bits.append(self.path)
        bits.append("\t")
        bits.append(_FALSE_TRUE[self.is_secure])
        bits.append("\t")
        bits.append(str(self.expiration))
        bits.append("\t")
        bits.append(self.name)
        bits.append("\t")
        bits.append(self.value)
        return "".join(bits)


def parse_cookie_string(cookie_string):
    cookie_string = cookie_string.strip()
    if cookie_string.startswith("#HttpOnly_"):
        http_only = True
        cookie_string = cookie_string[10:]
    else:
        http_only = False
    parts = cookie_string.split("\t")
    if len(parts) == 6:
        domain, include_subdomains, path, is_secure, expiration, name = parts
        value = ""
    else:
        domain, include_subdomains, path, is_secure, expiration, name, value = parts
    return Cookie(
        http_only, domain, include_subdomains == "TRUE", path, is_secure == "TRUE", int(expiration), name, value
    )


def parse_cookie_list_string(cookie_list_string):
    return [parse_cookie_string(line) for line in cookie_list_string.splitlines() if line.strip()]


def _has_header(name, headers, headers_list):
    name = name.lower()
    if headers and any(key.lower() == name for key in headers):
        return True
    return bool(headers_list) and any(line.partition(":")[0].strip().lower() == name for line in headers_list)


def _form_quote(value):
    return value.replace('"', "%22").replace("\r", "%0D").replace("\n", "%0A").encode("utf-8")


def _as_bytes(value):
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return str(value).encode("utf-8")


def _multipart(data, files):
    boundary = os.urandom(16).hex().encode()
    parts = []
    for name, value in data.items() if isinstance(data, dict) else data or ():
        parts += [b"--", boundary, b'\r\nContent-Disposition: form-data; name="', _form_quote(name), b'"\r\n\r\n']
        parts += [_as_bytes(value), b"\r\n"]
    for name, value in files.items() if isinstance(files, dict) else files:
        content_type = "application/octet-stream"
        if isinstance(value, tuple):
            filename, content = value[0], value[1]
            if len(value) > 2:
                content_type = value[2]
        else:
            filename, content = os.path.basename(getattr(value, "name", None) or name), value
        parts += [b"--", boundary, b'\r\nContent-Disposition: form-data; name="', _form_quote(name)]
        parts += [b'"; filename="', _form_quote(filename), b'"\r\nContent-Type: ', content_type.encode("latin-1")]
        parts += [b"\r\n\r\n", _as_bytes(content), b"\r\n"]
    parts += [b"--", boundary, b"--\r\n"]
    return b"".join(parts), "multipart/form-data; boundary=" + boundary.decode()


def session_cookie_for_url(
    url, name, value, http_only=False, include_subdomains=True, is_secure=False, include_url_path=False
):
    parsed = urlparse(url)
    path = parsed.path if include_url_path else "/"
    return Cookie(http_only, "." + parsed.hostname, include_subdomains, path, is_secure, 0, name, value)


def _encoding_from_content_type(content_type):
    if content_type is None:
        return "latin1"
    mime, _, params = content_type.partition(";")
    for param in params.split(";"):
        key, _, value = param.partition("=")
        value = value.strip().strip("'\"")
        if key.strip().lower() == "charset" and value:
            return value
    mime = mime.strip().lower()
    if mime == "application/json" or mime.endswith("+json"):
        return "utf-8"
    return "latin1"


def _cookie_list_to_cookie_dict(cookie_list):
    return {cookie.name: cookie.value for cookie in cookie_list}


class Request:
    __slots__ = "_method _url _header_list _cookie_list _auth _data".split()

    def __init__(self, method, url, header_list, cookie_list, auth, data):
        self._method = method
        self._url = url
        self._header_list = header_list
        self._cookie_list = cookie_list
        self._auth = auth
        self._data = data

    @property
    def method(self):
        return self._method

    @property
    def url(self):
        return self._url

    @property
    def header_list(self):
        return self._header_list

    @property
    def headers(self):
        return dict(header.split(": ", 1) for header in self.header_list)

    @property
    def cookie_list(self):
        return self._cookie_list

    @property
    def cookies(self):
        return _cookie_list_to_cookie_dict(self.cookie_list)

    @property
    def auth(self):
        return self._auth

    @property
    def data(self):
        return self._data


class Headers(collections.abc.Mapping):
    __slots__ = ("_items",)

    def __init__(self, pairs):
        items = {name.lower(): (name, value) for name, value in pairs}
        if len(items) != len(pairs):
            items = {}
            for name, value in pairs:
                key = name.lower()
                if key in items:
                    items[key] = (items[key][0], items[key][1] + ", " + value)
                else:
                    items[key] = (name, value)
        self._items = items

    def __getitem__(self, name):
        return self._items[name.lower()][1]

    def __contains__(self, name):
        return isinstance(name, str) and name.lower() in self._items

    def __iter__(self):
        return (name for name, _ in self._items.values())

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return repr(dict(self.items()))


class Response:
    __slots__ = (
        "_req",
        "_resp",
        "_start_time",
        "_end_time",
        "_redirect_url",
        "_prev",
        "_body",
        "_text",
        "_header",
        "_headers_tuple",
        "_headers",
        "_encoding",
        "_json",
    )

    def __init__(self, req, resp, start_time, end_time):
        self._req = req
        self._resp = resp
        self._start_time = start_time
        self._end_time = end_time

    @property
    def request(self):
        return self._req

    @property
    def status_code(self):
        return self._resp.get_response_code()

    response_code = status_code

    @property
    def ok(self):
        return self.status_code < 400

    @property
    def reason(self):
        status_line = self._final_header_block().partition("\r\n")[0]
        reason = status_line.split(" ", 2)[2].strip() if status_line.count(" ") >= 2 else ""
        if not reason:
            try:
                reason = http.HTTPStatus(self.status_code).phrase
            except ValueError:
                pass
        return reason

    @property
    def elapsed(self):
        return datetime.timedelta(seconds=self.total_time)

    def raise_for_status(self):
        if self.status_code >= 400:
            kind = "Client" if self.status_code < 500 else "Server"
            raise HTTPError("%d %s Error: %s for url: %s" % (self.status_code, kind, self.reason, self.url), self)

    @property
    def url(self):
        return self._resp.get_effective_url()

    @property
    def redirect_url(self):
        if not hasattr(self, "_redirect_url"):
            self._redirect_url = self._resp.get_redirect_url()
        return self._redirect_url

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def queue_time(self):
        return self._resp.get_queue_time()

    @property
    def client_lag(self):
        return self._end_time - self._start_time - self._resp.get_total_time() - self._resp.get_queue_time()

    @property
    def total_time(self):
        return self._resp.get_total_time()

    @property
    def namelookup_time(self):
        return self._resp.get_namelookup_time()

    @property
    def connect_time(self):
        return self._resp.get_connect_time()

    @property
    def appconnect_time(self):
        return self._resp.get_appconnect_time()

    @property
    def pretransfer_time(self):
        return self._resp.get_pretransfer_time()

    @property
    def posttransfer_time(self):
        return self._resp.get_posttransfer_time()

    @property
    def starttransfer_time(self):
        return self._resp.get_starttransfer_time()

    @property
    def upload_size(self):
        return self._resp.get_size_upload()

    @property
    def download_size(self):
        return self._resp.get_size_download()

    @property
    def primary_ip(self):
        return self._resp.get_primary_ip()

    @property
    def http_version(self):
        return self._resp.get_http_version()

    @property
    def cookielist(self):
        return [parse_cookie_string(cookie) for cookie in self._resp.get_cookielist()]

    @property
    def cookies(self):
        return _cookie_list_to_cookie_dict(self.cookielist)

    @property
    def history(self):
        result = []
        cur = getattr(self, "_prev", None)
        while cur is not None:
            result.append(cur)
            cur = getattr(cur, "_prev", None)
        result.reverse()
        return result

    @property
    def body(self):
        if not hasattr(self, "_body"):
            self._body = self._resp.get_body()
        return self._body

    @property
    def encoding(self):
        if not hasattr(self, "_encoding"):
            self._encoding = _encoding_from_content_type(self.headers.get("content-type"))
        return self._encoding

    @encoding.setter
    def encoding(self, encoding):
        self._encoding = encoding
        if hasattr(self, "_text"):
            del self._text

    @property
    def text(self):
        if not hasattr(self, "_text"):
            self._text = self.body.decode(self.encoding)
        return self._text

    def json(self):
        if not hasattr(self, "_json"):
            self._json = _json.loads(self.body.decode("utf-8"))
        return self._json

    @property
    def headers(self):
        if not hasattr(self, "_headers"):
            self._headers = Headers(self.headers_tuple)
        return self._headers

    @property
    def headers_tuple(self):
        if not hasattr(self, "_headers_tuple"):
            block = self._final_header_block()
            self._headers_tuple = tuple(
                [(name, value.strip()) for name, _, value in [line.partition(":") for line in block.split("\r\n")[1:]]]
            )
        return self._headers_tuple

    def _final_header_block(self):
        blocks = [b for b in self.header.split("\r\n\r\n") if b]
        return blocks[-1] if blocks else ""

    @property
    def header(self):
        if not hasattr(self, "_header"):
            self._header = self._resp.get_header().decode("latin-1")
        return self._header


class Session:
    def __init__(
        self,
        ae_loop,
        loop,
        connections="shared",
        tls="shared",
        http_version="HTTP/2",
        idle_timeout=118,
        max_connection_age=0,
        max_host_connections=0,
        max_connections=0,
        cert=None,
        verify=False,
        timeout=None,
        connect_timeout=None,
    ):
        self._loop = loop
        self._options = {"cert": cert, "verify": verify, "timeout": timeout, "connect_timeout": connect_timeout}
        self._session = _acurl.Session(
            ae_loop,
            connections,
            tls,
            http_version,
            idle_timeout,
            max_connection_age,
            max_host_connections,
            max_connections,
        )
        self._response_callback = None

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self.close()

    def cookies(self):
        return _cookie_list_to_cookie_dict(parse_cookie_string(c) for c in self._session.get_cookielist())

    async def get(self, url, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url, **kwargs):
        return await self.request("PATCH", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.request("DELETE", url, **kwargs)

    async def head(self, url, **kwargs):
        kwargs.setdefault("allow_redirects", False)
        return await self.request("HEAD", url, **kwargs)

    async def options(self, url, **kwargs):
        return await self.request("OPTIONS", url, **kwargs)

    async def request(
        self,
        method,
        url,
        params=None,
        headers=None,
        headers_list=None,
        cookies=None,
        cookie_list=None,
        auth=None,
        data=None,
        json=None,
        files=None,
        allow_redirects=True,
        max_redirects=5,
        cert=None,
        verify=None,
        timeout=None,
        connect_timeout=None,
    ):
        options = self._options
        if cert is not None or verify is not None or timeout is not None or connect_timeout is not None:
            options = dict(options)
            if cert is not None:
                options["cert"] = cert
            if verify is not None:
                options["verify"] = verify
            if timeout is not None:
                options["timeout"] = timeout
            if connect_timeout is not None:
                options["connect_timeout"] = connect_timeout
        if params:
            base, hash_, fragment = url.partition("#")
            query = params if isinstance(params, str) else urlencode(params, doseq=True)
            url = base + ("&" if "?" in base else "?") + query + hash_ + fragment

        content_type = None
        if json is not None:
            if data is not None or files is not None:
                raise ValueError("json cannot be combined with data or files")
            data = _json_encoder.encode(json)
            content_type = "application/json"
        elif files is not None:
            data, content_type = _multipart(data, files)
        elif isinstance(data, (dict, list, tuple)):
            data = urlencode(data, doseq=True)
            content_type = "application/x-www-form-urlencoded"
        if content_type is not None and not _has_header("content-type", headers, headers_list):
            headers_list = list(headers_list) if headers_list else []
            headers_list.append("Content-Type: " + content_type)

        if headers:
            if headers_list is None:
                headers_list = []
            headers_list.extend("%s: %s" % i for i in headers.items())

        if cookies:
            if cookie_list is None:
                cookie_list = []
            for k, v in cookies.items():
                cookie_list.append(session_cookie_for_url(url, k, v))

        return await self._request(
            method,
            url,
            tuple(headers_list) if headers_list else None,
            tuple(cookie_list) if cookie_list else None,
            auth,
            data,
            options,
            allow_redirects,
            max_redirects,
        )

    def set_response_callback(self, callback):
        self._response_callback = callback

    async def _request(
        self,
        method,
        url,
        header_tuple,
        cookie_tuple,
        auth,
        data,
        options,
        allow_redirects,
        remaining_redirects,
        prev=None,
    ):
        start_time = time.time()
        start = time.monotonic()
        request = Request(method, url, header_tuple, cookie_tuple, auth, data)

        future = self._loop.create_future()
        self._session.request(
            future,
            method,
            url,
            headers=header_tuple,
            cookies=tuple(c.format() for c in cookie_tuple) if cookie_tuple else None,
            auth=auth,
            data=data,
            **options,
        )
        resp = await future
        response = Response(request, resp, start_time, start_time + time.monotonic() - start)
        if prev is not None:
            response._prev = prev

        if self._response_callback:
            self._response_callback(response)
        if allow_redirects and (300 <= response.status_code < 400) and response.redirect_url is not None:
            if remaining_redirects == 0:
                raise RequestError("Max Redirects")
            elif response.status_code in {301, 302, 303}:
                redir_response = await self._request(
                    "GET",
                    response.redirect_url,
                    header_tuple,
                    None,
                    auth,
                    None,
                    options,
                    allow_redirects,
                    remaining_redirects - 1,
                    response,
                )
            else:
                redir_response = await self._request(
                    method,
                    response.redirect_url,
                    header_tuple,
                    None,
                    auth,
                    data,
                    options,
                    allow_redirects,
                    remaining_redirects - 1,
                    response,
                )
            return redir_response
        return response

    def erase_all_cookies(self):
        self._session.apply_cookielist(("ALL",))

    def erase_session_cookies(self):
        self._session.apply_cookielist(("SESS",))

    def get_cookie_list(self):
        return [parse_cookie_string(cookie) for cookie in self._session.get_cookielist()]

    def add_cookie_list(self, cookie_list):
        self._session.apply_cookielist(tuple(c.format() for c in cookie_list))


class EventLoop:
    def __init__(
        self,
        loop=None,
        same_thread=False,
        max_host_connections=0,
        max_total_connections=0,
        max_idle_connections=1000,
    ):
        self._running = False
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
        self._loop = loop
        self._ae_loop = _acurl.EventLoop(max_host_connections, max_total_connections, max_idle_connections)
        # Completed requests end up on the fd pipe, complete callback called
        self._loop.add_reader(self._ae_loop.get_out_fd(), self._complete)
        if same_thread:
            self._loop.call_later(0, self._same_thread_runner)
        else:
            self._run_in_thread()

    def _same_thread_runner(self):
        """Start event loop in normal python thread, allows use of python debugger and profiler"""
        self._ae_loop.once()
        self._loop.call_later(0.001, self._same_thread_runner)

    def _run_in_thread(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._runner, daemon=True)
            self._thread.start()

    def _runner(self):
        self._ae_loop.main()
        self._running = False

    def stop(self):
        if self._running:
            if not self._loop.is_closed():
                self._loop.remove_reader(self._ae_loop.get_out_fd())
            self._ae_loop.stop()

    def __del__(self):
        self.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.stop()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self.stop()

    def _complete(self):
        for error, response, future in self._ae_loop.get_completed():
            if response is not None:
                future.set_result(response)
            else:
                code, message = error
                future.set_exception((Timeout if code == CURLE_OPERATION_TIMEDOUT else RequestError)(message, code))

    def session(
        self,
        connections="shared",
        tls="shared",
        http_version="HTTP/2",
        idle_timeout=118,
        max_connection_age=0,
        max_host_connections=0,
        max_connections=0,
        cert=None,
        verify=False,
        timeout=None,
        connect_timeout=None,
    ):
        return Session(
            self._ae_loop,
            self._loop,
            connections,
            tls,
            http_version,
            idle_timeout,
            max_connection_age,
            max_host_connections,
            max_connections,
            cert,
            verify,
            timeout,
            connect_timeout,
        )
