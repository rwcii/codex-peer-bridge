#!/usr/bin/env python3
"""DeepSeek harness adapter: notice delivery and session facts.

Notice delivery is the bridge's `codex queue` analogue; `default_model` supplies
the model id a DeepSeek peer advertises in its peer name.

The DeepSeek harness has no `queue` subcommand, so a notice reaches a session
through its local HTTP RPC instead: `session/prompt` with `mode: "queue"`
resolves to the agent's `followup`, which is the same "wake this session with a
message" primitive that `codex queue` provides. `mode: "steer"` would resolve to
`steer` instead; notices deliberately use `queue` so an in-flight turn is never
interrupted by peer traffic.

The RPC listens on loopback and is gated by an authority-bound HMAC cookie. Its
signing secret is a per-user, mode-0600 file in the harness home, so an ordinary
same-user process can mint the cookie. That is what keeps this adapter inside a
standard-library-only runtime and avoids requiring an in-process plugin.

Wire contract, as implemented by the installed harness:

    POST <base>/api/session/prompt
    {"type":"client-request","rpcId":"...","method":"session/prompt",
     "payload":{"args":{"request":{
        "requestId":"...","sessionId":"...","mode":"queue",
        "content":[{"type":"text","text":"..."}],"clientTimeZone":"UTC"}}}}

    cookie name   "dsh-auth-" + base64url(sha256(authority))
    cookie value  "v1." + base64url(json payload) + "."
                  + base64url(HMAC-SHA256(secret, <the base64url body string>))

The signature covers the **encoded body string**, not the raw JSON, and the
signed `authority` must equal the `Host` header. Both details are load-bearing:
getting either wrong yields a plain 401 with no other diagnostic.

Scope note: the destination is constrained to loopback. The cookie is an
authentication credential for the local harness, and this module refuses to send
it anywhere else.
"""
import base64
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
from pathlib import Path
import socket
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

SECRET_BYTES = 32
COOKIE_PREFIX = 'dsh-auth-'
COOKIE_VERSION = 1
DEFAULT_TTL_MS = 60_000
MAX_CREDENTIAL_BYTES = 65536


class DeliveryError(RuntimeError):
    """The notice could not be handed to the harness."""


def b64url_encode(raw):
    """Unpadded base64url, matching the harness's own encoder."""
    return base64.urlsafe_b64encode(raw).decode().rstrip('=')


def b64url_decode(text):
    """Strict unpadded base64url decode; returns None when malformed.

    Rejecting non-canonical input matters because the same value is both decoded
    and re-encoded while verifying; a lenient decoder would accept a secret the
    harness itself would refuse. The empty string decodes to empty bytes, as the
    harness's own decoder does; callers that need a real secret check its length.
    """
    if not isinstance(text, str):
        return None
    if text == '':
        return b''
    if any(c not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_' for c in text):
        return None
    if len(text) % 4 == 1:
        return None
    padded = text + '=' * ((4 - len(text) % 4) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded)
    except (ValueError, base64.binascii.Error):
        return None
    return raw if b64url_encode(raw) == text else None


def _destination(base):
    """Parse and validate a harness base URL; return `(host, authority)`.

    The authority is derived from `netloc`, not from `hostname`, and the two must
    agree in canonical form. That single requirement closes the class of bug
    where the host being validated and the host being connected to differ:
    `http://evil.com\\@127.0.0.1` parses with `hostname == '127.0.0.1'` while an
    HTTP client connects using the netloc `evil.com\\@127.0.0.1`. A netloc
    carrying userinfo or any other non-canonical spelling is therefore refused,
    and IPv6 hosts keep the brackets the harness's own canonicalization expects.

    The parsed parts are returned so a caller builds its request from the very
    parse that was validated instead of re-parsing the raw string. A scheme-less
    `host[:port]` is accepted here, so echoing the raw value into an HTTP client
    would hand it a URL with no scheme and raise outside this module's error type.
    """
    try:
        parts = urllib.parse.urlsplit(base if '//' in base else 'http://' + base)
    except ValueError as exc:
        # urlsplit raises for a malformed bracketed host before any of the checks
        # below can run. Left bare, that ValueError would be neither a
        # DeliveryError nor an OSError, so it would escape the notifier's retry
        # handling and kill the notifier and its supervisor.
        raise DeliveryError(f'harness URL is malformed: {exc}') from exc
    if parts.scheme not in ('http', 'https'):
        raise DeliveryError(f'unsupported harness URL scheme: {parts.scheme}')
    netloc = parts.netloc
    if not netloc:
        raise DeliveryError('harness URL has no host')
    if '@' in netloc:
        raise DeliveryError('harness URL must not carry userinfo; the host would be ambiguous')
    host = parts.hostname
    if not host:
        raise DeliveryError('harness URL has no host')
    host = host.lower()
    try:
        port = parts.port
    except ValueError as exc:
        raise DeliveryError(f'harness URL has an invalid port: {exc}') from exc
    default = 443 if parts.scheme == 'https' else 80
    text = f'[{host}]' if ':' in host else host
    canonical = text if port in (None, default) else f'{text}:{port}'
    # An explicit default port is harmless: the harness strips it the same way.
    accepted = {canonical} | ({f'{text}:{default}'} if port == default else set())
    if netloc.lower() not in accepted:
        raise DeliveryError(f'harness URL authority {netloc!r} is not in canonical form')
    if parts.path not in ('', '/'):
        # The endpoint is appended to the server root. Accepting a path would build a
        # doubled request path such as `/api/api/session/prompt`, so it is refused
        # rather than silently producing a URL the harness cannot route.
        raise DeliveryError(f'harness URL must be an origin with no path, got {parts.path!r}')
    return parts, host, canonical


def authority(base):
    """Canonical `host[:port]` authority for a harness base URL.

    Matches the harness's `new URL('http://' + host).host`: the hostname is
    lowercased, a default port is dropped, and IPv6 hosts keep their brackets.
    The result is both the cookie-name input and the signed audience, so it is
    produced exactly as the harness produces it.
    """
    return _destination(base)[2]


def target(base):
    """Validate one delivery destination and pin the address to connect to.

    Returns `(scheme, authority, address, port, path)`. Resolution happens once,
    here, and the caller sends to the returned `address`, so a resolver cannot
    answer loopback for the check and something else for the connection. The
    request's `Host` header still carries the canonical authority, which is what
    the harness derives the cookie name from, so pinning the address does not
    change the signed audience.

    The host must be a loopback IP literal or the bare name `localhost`, and every
    address it resolves to must be loopback. Requiring both matches the harness's
    own host fence, which accepts `localhost` and 127/8 literals but not lookalikes
    such as `evil.localhost` or the numeric form `2130706433`. A name that does not
    resolve is refused rather than assumed local.
    """
    parts, host, canonical = _destination(base)
    host_ok = host == 'localhost'
    if not host_ok:
        try:
            host_ok = ipaddress.ip_address(host).is_loopback
        except ValueError:
            host_ok = False
    if not host_ok:
        raise DeliveryError(f'refusing to send a harness credential to non-loopback host {host!r}')
    default = 443 if parts.scheme == 'https' else 80
    port = parts.port if parts.port is not None else default
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise DeliveryError(f'cannot resolve harness host {host!r}: {exc}') from exc
    resolved = []
    for info in infos:
        address = info[4][0]
        try:
            if ipaddress.ip_address(address).is_loopback:
                resolved.append(address)
                continue
        except ValueError:
            pass
        raise DeliveryError(f'refusing to send a harness credential to non-loopback host {host!r}'
                            f' (resolves to {address})')
    if not resolved:
        raise DeliveryError(f'harness host {host!r} resolved to no address')
    # All verified loopback addresses are returned so a caller can try each in turn;
    # the port comes from the authority rather than from the resolution, so the
    # connection can never be aimed at a port the signed authority does not name.
    return parts.scheme, canonical, tuple(resolved), port


def require_loopback(base):
    """Reject any harness URL that is not loopback.

    The signed cookie authenticates this user to the local harness. Sending it to
    a remote authority would disclose that credential, so a non-loopback
    destination is refused before any secret is read. See `target` for the checks,
    which it shares with delivery so validation and transport cannot disagree.
    """
    target(base)


def _yaml_block(text, key):
    """Return the lines nested under one mapping key, or None.

    This is a deliberately narrow reader rather than a YAML implementation. The
    files it reads are short and generated by the harness, and a full parser
    would be a new dependency for two lookups. Everything that is read is
    validated afterwards, so a shape this reader misreads fails closed.
    """
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != key + ':':
            continue
        indent = len(line) - len(line.lstrip())
        block = []
        for later in lines[index + 1:]:
            if later.strip() and len(later) - len(later.lstrip()) <= indent:
                break
            block.append(later)
        return block
    return None


def _scalar(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    return value


def secret(path):
    """Read the browser-session signing secret from the harness credential file.

    The file is a same-user credential store, so ownership and mode are checked
    before its contents are used, in the same spirit as the peer key reader in
    `bridge.py`. The returned value is the raw 32 bytes the HMAC is keyed with;
    the file stores them base64url-encoded.
    """
    path = Path(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        raise DeliveryError(f'harness credential file not found: {path}') from exc
    except OSError as exc:
        # O_NOFOLLOW makes a symlinked credential file fail here rather than
        # silently following it to a file the user did not designate.
        raise DeliveryError(f'cannot open harness credential file {path}: {exc.strerror}') from exc
    with os.fdopen(fd) as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise DeliveryError('harness credential file is not a regular file')
        if info.st_uid != os.getuid():
            raise DeliveryError('harness credential file belongs to another user')
        if info.st_mode & 0o077:
            raise DeliveryError('harness credential file must not be group or world accessible')
        if info.st_size > MAX_CREDENTIAL_BYTES:
            raise DeliveryError('harness credential file is unexpectedly large')
        text = handle.read()
    block = _yaml_block(text, 'client-connection/browser-session')
    if block is None:
        raise DeliveryError('harness credential file has no browser-session record')
    encoded = None
    kind = None
    version = None
    for line in block:
        key, separator, value = line.strip().partition(':')
        if not separator:
            continue
        key = key.strip()
        if key == 'secret':
            encoded = _scalar(value)
        elif key == 'kind':
            kind = _scalar(value)
        elif key == 'version':
            version = _scalar(value)
    # Validate the record the way the harness's own reader does, rather than
    # accepting any `secret` line that happens to sit in this block.
    if kind not in (None, 'grant'):
        raise DeliveryError(f'unsupported browser-session record kind: {kind}')
    if version not in (None, '1'):
        raise DeliveryError(f'unsupported browser-session secret version: {version}')
    if encoded is None:
        raise DeliveryError('browser-session record has no secret')
    raw = b64url_decode(encoded)
    if raw is None or len(raw) != SECRET_BYTES:
        raise DeliveryError(f'browser-session secret must be {SECRET_BYTES} base64url bytes')
    return raw


def cookie(key, audience, now_ms=None, ttl_ms=DEFAULT_TTL_MS):
    """Mint an authority-bound session cookie.

    @param key - raw signing secret bytes.
    @param audience - exact authority the cookie will be presented to.
    @returns the `(name, value)` pair to send as one Cookie header field.
    """
    issued = int(time.time() * 1000) if now_ms is None else int(now_ms)
    payload = {'version': COOKIE_VERSION, 'authority': audience,
               'issuedAt': issued, 'expiresAt': issued + int(ttl_ms)}
    body = b64url_encode(json.dumps(payload, separators=(',', ':')).encode())
    signature = b64url_encode(hmac.new(key, body.encode(), hashlib.sha256).digest())
    name = COOKIE_PREFIX + b64url_encode(hashlib.sha256(audience.encode()).digest())
    return name, f'v1.{body}.{signature}'


def settings_path(home=None):
    """Harness `settings.yaml`, which holds the configured default model.

    An explicit empty home means "no harness home"; only an omitted argument
    falls back to `DSH_HOME`.
    """
    home = os.environ.get('DSH_HOME') if home is None else home
    return (Path(home) / 'settings.yaml') if home else None


def default_model(path=None):
    """The harness's configured default model id, or None when unavailable.

    The model a *running* session is actually served by is not exposed: the
    session RPC surface offers only `selectModel` to set it and `modelCatalog`
    to list what exists, and the session log is zstd-compressed, which the
    standard library cannot read. The configured default is therefore the one
    reliable local source, and an explicit model argument overrides it.

    Every failure returns None rather than raising: an unreadable or unexpected
    settings file must not stop a session from registering, it only means the
    peer name carries no model segment.
    """
    target = Path(path) if path else settings_path()
    if target is None:
        return None
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return None
    try:
        with os.fdopen(fd) as handle:
            text = handle.read(MAX_CREDENTIAL_BYTES)
    except OSError:
        return None
    block = _yaml_block(text, 'agent-default-model')
    if block is None:
        return None
    for line in block:
        key, separator, value = line.strip().partition(':')
        if separator and key.strip() == 'model':
            return _scalar(value) or None
    return None


def request_body(session_id, text, timezone='UTC'):
    """Build the `session/prompt` envelope for one queued notice.

    `args` must contain exactly the single wire field `request`; the harness
    rejects missing or extra argument names before the method is reached.
    """
    if not isinstance(text, str) or not text.strip():
        raise DeliveryError('notice text must be non-empty')
    return {'type': 'client-request', 'rpcId': str(uuid.uuid4()), 'method': 'session/prompt',
            'payload': {'args': {'request': {
                'requestId': str(uuid.uuid4()), 'sessionId': session_id, 'mode': 'queue',
                'content': [{'type': 'text', 'text': text}], 'clientTimeZone': timezone}}}}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward a harness credential beyond the validated destination."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def deliver(base, session_id, text, credentials=None, timeout=15, opener=None):
    """Queue one notice into a harness session.

    @param base - harness base URL, such as `http://127.0.0.1:51992`.
    @param session_id - exact target session identity.
    @param text - notice body. Notices are content-free by construction.
    @param credentials - harness `.credentials.yaml`; required.
    @returns the decoded `value` object, which reports `accepted`.
    @throws DeliveryError for a refused, malformed, or failed delivery.
    """
    if not base:
        raise DeliveryError('harness URL is required (set DSH_WEB_URL or pass --dsh-url)')
    if credentials is None:
        raise DeliveryError('harness credential file is required (set DSH_HOME or pass --dsh-credentials)')
    # Validate and resolve once, then build the request from that same parse. The
    # raw argument is never echoed into the HTTP client: a scheme-less `host:port`
    # is accepted by validation, and handing it to urllib would raise a bare
    # ValueError that is neither a DeliveryError nor an OSError, escaping the
    # notifier's retry handling and killing the notifier and its supervisor.
    scheme, audience, addresses, port = target(base)
    name, value = cookie(secret(credentials), audience)
    payload = json.dumps(request_body(session_id, text), ensure_ascii=True).encode()
    # The request goes to the exact address that was verified, so a second
    # resolution cannot redirect it. The explicit Host header carries the
    # canonical authority, which is what the harness derives the cookie name from,
    # so pinning the address does not change the signed audience.
    #
    # Every verified address is tried in order. Pinning to a single address would
    # drop the fallback an HTTP client normally provides, and a host can resolve to
    # several loopback addresses while the harness listens on only one family -
    # `localhost` commonly answers `::1` first on a host whose harness binds IPv4
    # only. Each attempt still goes to a pre-verified address, so the resolution
    # cannot be flipped between the check and the connection.
    # Environment proxies and automatic redirects would bypass address validation.
    send = opener or urllib.request.build_opener(
        urllib.request.ProxyHandler({}), NoRedirect()).open
    last = None
    for address in addresses:
        host_text = f'[{address}]' if ':' in address else address
        url = f'{scheme}://{host_text}:{port}/api/session/prompt'
        request = urllib.request.Request(url, data=payload, method='POST', headers={
            'Content-Type': 'application/json',
            'Cookie': f'{name}={value}',
            'Host': audience,
            'Content-Length': str(len(payload)),
        })
        # No Origin and no Sec-Fetch-Site: the harness's host fence accepts an absent
        # Origin and only rejects a cross-site fetch marker. urllib adds neither.
        try:
            with send(request, timeout=timeout) as response:
                if response.status != 200:
                    raise DeliveryError(f'harness returned HTTP {response.status}')
                document = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                # The harness answered and rejected this credential; another address
                # reaches the same server and would fail identically.
                raise DeliveryError(f'harness refused the notice (HTTP {exc.code}); '
                                    'the session cookie was not accepted') from exc
            if exc.code >= 500:
                # A server-side failure can be specific to this address, so the walk
                # continues rather than treating it as proof of unreachability.
                last = exc
                continue
            raise DeliveryError(f'harness returned HTTP {exc.code}') from exc
        except urllib.error.URLError as exc:
            last = exc
            continue
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            # A body that is not UTF-8 JSON may come from something else answering on
            # this address, so the next verified address is still worth trying. The
            # decode can fail before json.loads, so both errors are handled.
            last = exc
            continue
        except http.client.HTTPException as exc:
            # A service that is not the harness can answer with a malformed status line,
            # which urllib does not convert to URLError. HTTPException is not an OSError,
            # so left uncaught it would escape the notifier's retry handling and kill the
            # notifier and its supervisor. Retrying matches the malformed-body case.
            last = exc
            continue
        if not isinstance(document, dict):
            raise DeliveryError('harness returned an unexpected response')
        # Failures arrive inside a 200 envelope, so the envelope is unwrapped rather
        # than trusting the status code.
        if document.get('type') == 'server-response' and isinstance(document.get('result'), dict):
            document = document['result']
        if document.get('ok') is True and isinstance(document.get('value'), dict):
            return document['value']
        error = document.get('error')
        if isinstance(error, dict):
            code = error.get('code') or 'unknown'
            message = error.get('message') or 'no detail'
            raise DeliveryError(f'harness rejected the notice: {code}: {message}')
        raise DeliveryError('harness returned an unrecognized envelope')
    reason = getattr(last, 'reason', last)
    # The reason can be text a non-harness service chose, and notify.py prints this, so
    # it is bounded rather than echoed whole into the journal.
    detail = f'{type(reason).__name__}: {reason}'
    if len(detail) > 200:
        detail = detail[:200] + '...'
    raise DeliveryError(f'no verified address of {scheme}://{audience} accepted the notice: '
                        f'{detail}') from last
