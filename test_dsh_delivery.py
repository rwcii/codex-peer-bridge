import base64
import hashlib
import http.client
import hmac
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import unittest
from unittest.mock import patch
import urllib.error

import dsh_delivery

SECRET = bytes(range(32))
SECRET_TEXT = base64.urlsafe_b64encode(SECRET).decode().rstrip('=')


def credential_file(root, secret=SECRET_TEXT, mode=0o600, record='client-connection/browser-session'):
    path = Path(root) / '.credentials.yaml'
    path.write_text(f'version: 1\nrecords:\n  {record}:\n    kind: grant\n    payload:\n'
                    f'      version: 1\n      secret: {secret}\n'
                    'refs:\n  DEEPSEEK_API_KEY: unused-fixture-value\n')
    os.chmod(path, mode)
    return path


class Base64UrlTests(unittest.TestCase):
    def test_round_trip_is_unpadded(self):
        for raw in (b'', b'a', bytes(range(32))):
            encoded = dsh_delivery.b64url_encode(raw)
            self.assertNotIn('=', encoded)
            self.assertEqual(dsh_delivery.b64url_decode(encoded), raw)

    def test_empty_decodes_to_empty(self):
        # The harness's decoder accepts the empty string, so the length check
        # that rejects a too-short secret must be the thing that refuses it.
        self.assertEqual(dsh_delivery.b64url_decode(''), b'')

    def test_non_canonical_input_is_rejected(self):
        for bad in ('a', '====', 'ab==', 'a+b', 'a/b', 'a b', None, 7, b'YWJj'):
            self.assertIsNone(dsh_delivery.b64url_decode(bad))


class AuthorityTests(unittest.TestCase):
    def test_port_is_kept_and_default_port_is_dropped(self):
        self.assertEqual(dsh_delivery.authority('http://127.0.0.1:51992'), '127.0.0.1:51992')
        self.assertEqual(dsh_delivery.authority('http://127.0.0.1:80'), '127.0.0.1')
        self.assertEqual(dsh_delivery.authority('https://127.0.0.1:443'), '127.0.0.1')

    def test_hostname_is_lowercased(self):
        self.assertEqual(dsh_delivery.authority('http://LocalHost:51992'), 'localhost:51992')

    def test_bare_authority_is_accepted(self):
        self.assertEqual(dsh_delivery.authority('127.0.0.1:51992'), '127.0.0.1:51992')

    def test_ipv6_authority_keeps_brackets(self):
        # The harness derives the cookie name from `new URL('http://' + host).host`,
        # which keeps IPv6 brackets; an unbracketed authority can never match and
        # would produce a permanent, silent 401.
        self.assertEqual(dsh_delivery.authority('http://[::1]:51992'), '[::1]:51992')
        self.assertEqual(dsh_delivery.authority('http://[::1]:80'), '[::1]')
        self.assertEqual(dsh_delivery.authority('http://[::ffff:127.0.0.1]'), '[::ffff:127.0.0.1]')

    def test_explicit_default_port_is_accepted_and_stripped(self):
        self.assertEqual(dsh_delivery.authority('http://127.0.0.1:80'), '127.0.0.1')
        self.assertEqual(dsh_delivery.authority('https://127.0.0.1:443'), '127.0.0.1')

    def test_userinfo_is_refused(self):
        # A netloc whose host differs from its hostname is the shape where the host
        # being validated and the host being connected to can disagree.
        for bad in ('http://user@evil.com', 'http://127.0.0.1@evil.com'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)

    def test_backslash_netloc_is_refused(self):
        # urlsplit reports hostname 127.0.0.1 here, but urllib's HTTP connection
        # takes the whole literal netloc as the connection host. Rejecting any
        # non-canonical netloc closes that divergence.
        for bad in ('http://evil.example.com\\@127.0.0.1:51992',
                    'http://127.0.0.1\\@evil.com',
                    'http://evil.com\\@127.0.0.1'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.require_loopback(bad)

    def test_invalid_port_is_a_clean_delivery_error(self):
        # A bare ValueError here would escape notify.py's retry handling and kill
        # the notifier instead of retrying.
        for bad in ('http://127.0.0.1:99999', 'http://127.0.0.1:abc', 'http://127.0.0.1:-1'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)

    def test_missing_host_is_refused(self):
        for bad in ('http://', 'http:///', 'file:///etc/passwd', 'http://:51992'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)

    def test_malformed_bracketed_host_is_a_clean_delivery_error(self):
        # urlsplit raises ValueError for these itself, before any port handling.
        # Left bare that would be neither a DeliveryError nor an OSError, so it
        # would escape notify.py's retry handling and kill the notifier.
        for bad in ('http://[::1', 'http://[::1]]', 'http://[', 'http://[]',
                    'http://[evil.com]:1', 'http://[::1]@1', 'http://[1:2:3:4:5:6:7:8:9]'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.require_loopback(bad)

    def test_localhost_lookalikes_are_refused(self):
        # The harness's own fence accepts `localhost` and 127/8 literals only, so
        # accepting a lookalike here would send the credential to a name the
        # harness would refuse, resolved by whatever the local resolver says.
        for bad in ('http://evil.localhost:51992', 'http://localhost.evil.com',
                    'http://2130706433', 'http://0177.0.0.1', 'http://0x7f000001',
                    'http://127.1', 'http://0.0.0.0', 'http://[::]'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.require_loopback(bad)

    def test_loopback_is_required(self):
        for good in ('http://127.0.0.1:51992', 'http://localhost:1', 'http://[::1]:2', 'http://127.5.5.5:9'):
            dsh_delivery.require_loopback(good)
        for bad in ('http://example.com', 'http://10.0.0.1:51992', 'http://192.168.1.5'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.require_loopback(bad)


class SecretTests(unittest.TestCase):
    def test_valid_secret_file_is_read(self):
        with tempfile.TemporaryDirectory() as temp:
            self.assertEqual(dsh_delivery.secret(credential_file(temp)), SECRET)

    def test_permissive_mode_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(credential_file(temp, mode=0o644))

    def test_missing_record_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path = credential_file(temp, record='client-connection/other')
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(path)

    def test_wrong_length_secret_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            short = base64.urlsafe_b64encode(b'short').decode().rstrip('=')
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(credential_file(temp, secret=short))

    def test_symlink_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            real = credential_file(temp)
            link = Path(temp) / 'link.yaml'
            link.symlink_to(real)
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(link)

    def test_wrong_record_kind_or_version_is_refused(self):
        # Match the harness's own record validation instead of accepting any
        # `secret` line inside the block.
        with tempfile.TemporaryDirectory() as temp:
            path = credential_file(temp)
            text = path.read_text().replace('kind: grant', 'kind: borrow')
            path.write_text(text)
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(path)
        with tempfile.TemporaryDirectory() as temp:
            path = credential_file(temp)
            text = path.read_text().replace('      version: 1', '      version: 2')
            path.write_text(text)
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(path)

    def test_missing_file_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.secret(Path(temp) / 'absent.yaml')


class CookieTests(unittest.TestCase):
    def test_cookie_is_verifiable_and_authority_bound(self):
        audience = '127.0.0.1:51992'
        name, value = dsh_delivery.cookie(SECRET, audience, now_ms=1000, ttl_ms=500)
        expected_name = ('dsh-auth-' + base64.urlsafe_b64encode(
            hashlib.sha256(audience.encode()).digest()).decode().rstrip('='))
        self.assertEqual(name, expected_name)
        version, body, signature = value.split('.')
        self.assertEqual(version, 'v1')
        # The harness signs the encoded body string, not the raw JSON.
        self.assertEqual(base64.urlsafe_b64encode(
            hmac.new(SECRET, body.encode(), hashlib.sha256).digest()).decode().rstrip('='), signature)
        payload = json.loads(base64.urlsafe_b64decode(body + '=' * (-len(body) % 4)))
        self.assertEqual(payload['authority'], audience)
        self.assertEqual(payload['version'], 1)
        self.assertEqual(payload['issuedAt'], 1000)
        self.assertEqual(payload['expiresAt'], 1500)

    def test_different_authorities_get_different_cookie_names(self):
        first, _ = dsh_delivery.cookie(SECRET, '127.0.0.1:51992')
        second, _ = dsh_delivery.cookie(SECRET, 'localhost:51992')
        self.assertNotEqual(first, second)


class RequestBodyTests(unittest.TestCase):
    def test_envelope_matches_the_documented_contract(self):
        body = dsh_delivery.request_body('session-abc', 'hello')
        self.assertEqual(body['type'], 'client-request')
        self.assertEqual(body['method'], 'session/prompt')
        args = body['payload']['args']
        # The gateway rejects missing or extra argument names.
        self.assertEqual(list(args), ['request'])
        request = args['request']
        self.assertEqual(request['sessionId'], 'session-abc')
        self.assertEqual(request['mode'], 'queue')
        self.assertEqual(request['content'], [{'type': 'text', 'text': 'hello'}])
        self.assertEqual(request['clientTimeZone'], 'UTC')
        self.assertTrue(request['requestId'])

    def test_empty_notice_is_refused(self):
        for bad in ('', '   ', None):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.request_body('session-abc', bad)


class FakeResponse:
    def __init__(self, document, status=200):
        self.status = status
        self._body = json.dumps(document).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class RawResponse(FakeResponse):
    """A response whose body is supplied as bytes, for decode-failure cases."""

    def __init__(self, body, status=200):
        self.status = status
        self._body = body


TWO_LOOPBACK = [
    (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 51992, 0, 0)),
    (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 51992)),
]


class DeliverTests(unittest.TestCase):
    def deliver(self, document, opener):
        return dsh_delivery.deliver('http://127.0.0.1:51992', 'session-abc', 'notice',
                                    credentials=self.credentials, opener=opener)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.credentials = credential_file(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_accepted_envelope_is_returned(self):
        seen = {}

        def opener(request, timeout=None):
            seen['url'] = request.full_url
            seen['method'] = request.get_method()
            seen['cookie'] = request.get_header('Cookie')
            seen['content_type'] = request.get_header('Content-type')
            seen['origin'] = request.get_header('Origin')
            seen['fetch_site'] = request.get_header('Sec-fetch-site')
            seen['body'] = json.loads(request.data)
            return FakeResponse({'type': 'server-response', 'rpcId': 'x',
                                 'result': {'ok': True, 'value': {'accepted': True}}})

        result = self.deliver(None, opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(seen['url'], 'http://127.0.0.1:51992/api/session/prompt')
        self.assertEqual(seen['method'], 'POST')
        self.assertEqual(seen['content_type'], 'application/json')
        self.assertIn('dsh-auth-', seen['cookie'])
        # The harness tolerates an absent Origin and rejects a cross-site marker.
        self.assertIsNone(seen['origin'])
        self.assertIsNone(seen['fetch_site'])
        self.assertEqual(seen['body']['payload']['args']['request']['mode'], 'queue')

    def test_remote_failure_code_is_reported(self):
        def opener(request, timeout=None):
            return FakeResponse({'ok': False, 'error': {'code': 'session/not-found', 'message': 'no such session'}})

        with self.assertRaises(dsh_delivery.DeliveryError) as caught:
            self.deliver(None, opener)
        self.assertIn('session/not-found', str(caught.exception))

    def test_unauthorized_is_distinguished(self):
        def opener(request, timeout=None):
            raise urllib.error.HTTPError(request.full_url, 401, 'Unauthorized', {}, None)

        with self.assertRaises(dsh_delivery.DeliveryError) as caught:
            self.deliver(None, opener)
        self.assertIn('401', str(caught.exception))

    def test_unreachable_harness_is_reported(self):
        def opener(request, timeout=None):
            raise urllib.error.URLError('connection refused')

        with self.assertRaises(dsh_delivery.DeliveryError):
            self.deliver(None, opener)

    def test_malformed_response_is_reported(self):
        class Broken(FakeResponse):
            def read(self):
                return b'not json'

        with self.assertRaises(dsh_delivery.DeliveryError):
            self.deliver(None, lambda request, timeout=None: Broken(None))

    def test_credentials_and_url_are_required(self):
        opener = lambda request, timeout=None: FakeResponse({'ok': True, 'value': {}})
        with self.assertRaises(dsh_delivery.DeliveryError):
            dsh_delivery.deliver(None, 'session-abc', 'notice', credentials=self.credentials, opener=opener)
        with self.assertRaises(dsh_delivery.DeliveryError):
            dsh_delivery.deliver('http://127.0.0.1:1', 'session-abc', 'notice', credentials=None, opener=opener)

    def test_scheme_less_url_is_built_from_the_validated_parse(self):
        # Validation accepts a bare `host[:port]`, so echoing the raw argument into
        # the HTTP client would hand it a URL with no scheme and raise a bare
        # ValueError - not a DeliveryError or an OSError - which escapes the
        # notifier's retry handling and kills the notifier and its supervisor.
        seen = {}

        def opener(request, timeout=None):
            seen['url'] = request.full_url
            seen['host'] = request.get_header('Host')
            return FakeResponse({'ok': True, 'value': {'accepted': True}})

        result = dsh_delivery.deliver('127.0.0.1', 'session-abc', 'notice',
                                      credentials=self.credentials, opener=opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(seen['url'], 'http://127.0.0.1:80/api/session/prompt')
        self.assertEqual(seen['host'], '127.0.0.1')

    def test_no_odd_url_form_raises_a_bare_value_error(self):
        for base in ('127.0.0.1', '127.0.0.1#51992', '127.0.0.1/51992', '127.0.0.1?51992',
                     'localhost:51992', 'http://[::1', 'http://[]', 'http://[evil.com]:1'):
            try:
                dsh_delivery.deliver(base, 'session-abc', 'notice', credentials=self.credentials,
                                     opener=lambda *a, **k: FakeResponse(None))
            except dsh_delivery.DeliveryError:
                pass
            except Exception as exc:  # noqa: BLE001 - the point is the exception type
                self.fail(f'{base!r} raised {type(exc).__name__} instead of DeliveryError: {exc}')

    def test_request_goes_to_the_verified_address_with_the_signed_authority(self):
        # Resolution happens once; the request must target exactly the address that
        # was checked, while the Host header keeps the canonical authority so the
        # harness still derives the same cookie name.
        seen = {}

        def opener(request, timeout=None):
            seen['url'] = request.full_url
            seen['host'] = request.get_header('Host')
            return FakeResponse({'ok': True, 'value': {}})

        dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                            credentials=self.credentials, opener=opener)
        self.assertEqual(seen['host'], 'localhost:51992')
        self.assertRegex(seen['url'], r'^http://(127\.0\.0\.1|\[::1\]):51992/api/session/prompt$')

    def test_every_resolved_address_is_tried_in_order(self):
        # A host can resolve to several loopback addresses while the harness listens
        # on only one family - `localhost` commonly answers `::1` first on a host
        # whose harness binds IPv4 only. Pinning to a single address would never
        # deliver for such a host; every attempt must still be a verified address.
        attempts = []

        def opener(request, timeout=None):
            attempts.append(request.full_url)
            if len(attempts) == 1:
                raise urllib.error.URLError(ConnectionRefusedError(61, 'Connection refused'))
            return FakeResponse({'ok': True, 'value': {'accepted': True}})

        with patch('dsh_delivery.socket.getaddrinfo', return_value=[
                (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 51992, 0, 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 51992))]):
            result = dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                                          credentials=self.credentials, opener=opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(len(attempts), 2)
        self.assertIn('[::1]', attempts[0])
        self.assertIn('127.0.0.1', attempts[1])

    def test_an_http_error_does_not_try_another_address(self):
        # An HTTP status proves the harness answered, so another address would reach
        # the same server and fail identically.
        attempts = []

        def opener(request, timeout=None):
            attempts.append(request.full_url)
            raise urllib.error.HTTPError(request.full_url, 401, 'Unauthorized', {}, None)

        with patch('dsh_delivery.socket.getaddrinfo', return_value=[
                (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 51992, 0, 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 51992))]):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                                     credentials=self.credentials, opener=opener)
        self.assertEqual(len(attempts), 1)

    def test_all_addresses_unreachable_reports_the_authority(self):
        def opener(request, timeout=None):
            raise urllib.error.URLError(ConnectionRefusedError(61, 'Connection refused'))

        with patch('dsh_delivery.socket.getaddrinfo', return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 51992))]):
            with self.assertRaises(dsh_delivery.DeliveryError) as caught:
                dsh_delivery.deliver('http://127.0.0.1:51992', 'session-abc', 'notice',
                                     credentials=self.credentials, opener=opener)
        self.assertIn('127.0.0.1:51992', str(caught.exception))

    def test_a_url_with_a_path_is_refused(self):
        # The endpoint is appended to the origin, so a path would build a doubled
        # request path such as /api/api/session/prompt.
        for bad in ('http://127.0.0.1:51992/api', 'http://127.0.0.1:51992/anything'):
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.authority(bad)

    def test_a_non_utf8_body_is_a_clean_delivery_error(self):
        # response.read().decode() runs before json.loads, so a non-UTF-8 body raises
        # UnicodeDecodeError - a ValueError, but neither a DeliveryError nor an
        # OSError - which would escape the notifier's retry handling and kill it.
        for body in (b'\xff\xfe not utf-8', b'latin-1 caf\xe9 body', b'\xff\xfe\x00j\x00s'):
            def opener(request, timeout=None, _body=body):
                return RawResponse(_body)
            with self.assertRaises(dsh_delivery.DeliveryError):
                self.deliver(None, opener)

    def test_a_server_error_on_one_address_retries_the_next(self):
        # A 5xx can be specific to one address, so the walk continues rather than
        # treating it as proof the harness is unreachable.
        attempts = []

        def opener(request, timeout=None):
            attempts.append(request.full_url)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(request.full_url, 503, 'Service Unavailable', {}, None)
            return FakeResponse({'ok': True, 'value': {'accepted': True}})

        with patch('dsh_delivery.socket.getaddrinfo', return_value=TWO_LOOPBACK):
            result = dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                                          credentials=self.credentials, opener=opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(len(attempts), 2)

    def test_a_malformed_body_on_one_address_retries_the_next(self):
        attempts = []

        def opener(request, timeout=None):
            attempts.append(request.full_url)
            if len(attempts) == 1:
                return RawResponse(b'<html>not the harness</html>')
            return FakeResponse({'ok': True, 'value': {'accepted': True}})

        with patch('dsh_delivery.socket.getaddrinfo', return_value=TWO_LOOPBACK):
            result = dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                                          credentials=self.credentials, opener=opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(len(attempts), 2)

    def test_an_http_layer_failure_is_a_clean_delivery_error(self):
        # A service that is not the harness answers with a malformed status line. urllib
        # leaves that as http.client.BadStatusLine, which is an HTTPException and NOT an
        # OSError, so unhandled it would escape the notifier's retry handling and kill
        # the notifier and its supervisor. Reachability with a real urlopen and a real
        # listener was confirmed independently; this pins the handling itself, and
        # asserts the walking behaviour on top of it.
        attempts = []

        def opener(request, timeout=None):
            attempts.append(request.full_url)
            if len(attempts) == 1:
                raise http.client.BadStatusLine('HELLO FROM A NON-HARNESS SERVICE')
            return FakeResponse({'ok': True, 'value': {'accepted': True}})

        with patch('dsh_delivery.socket.getaddrinfo', return_value=TWO_LOOPBACK):
            result = dsh_delivery.deliver('http://localhost:51992', 'session-abc', 'notice',
                                          credentials=self.credentials, opener=opener)
        self.assertEqual(result, {'accepted': True})
        self.assertEqual(len(attempts), 2, 'a malformed status line should try the next address')

    def test_a_long_remote_reason_is_bounded(self):
        # The reason can be text a non-harness service chose, and notify.py prints the
        # message, so it must not be echoed whole into the journal.
        def opener(request, timeout=None):
            raise http.client.BadStatusLine('A' * 4000)

        with self.assertRaises(dsh_delivery.DeliveryError) as caught:
            self.deliver(None, opener)
        self.assertLess(len(str(caught.exception)), 400)

    def test_non_loopback_destination_is_refused_before_reading_the_secret(self):
        with self.assertRaises(dsh_delivery.DeliveryError):
            dsh_delivery.deliver('http://example.com', 'session-abc', 'notice',
                                 credentials=self.credentials, opener=lambda *a, **k: FakeResponse(None))

    def test_no_request_is_ever_made_to_a_hostile_authority(self):
        # The regression that matters: these inputs were previously accepted, so a
        # request carrying the harness credential was actually emitted. The opener
        # must never be reached.
        sent = []

        def opener(request, timeout=None):
            sent.append(request.full_url)
            raise AssertionError('a request was sent')

        hostile = [
            'http://evil.example.com\\@127.0.0.1:51992',
            'http://127.0.0.1\\@evil.com',
            'http://user@evil.com',
            'http://evil.com',
            'http://127.0.0.1:99999',
            'http://127.0.0.1:abc',
            'ftp://127.0.0.1',
            'http://2130706433',
            'http://127.0.0.1.evil.com',
        ]
        for base in hostile:
            with self.assertRaises(dsh_delivery.DeliveryError):
                dsh_delivery.deliver(base, 'session-abc', 'notice',
                                     credentials=self.credentials, opener=opener)
        self.assertEqual(sent, [])


class DirectTransportTests(unittest.TestCase):
    def test_redirects_are_rejected_and_environment_proxies_are_ignored(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                requests.append(self.command)
                self.rfile.read(int(self.headers['Content-Length']))
                if self.server.redirect:
                    self.send_response(302)
                    self.send_header('Location', '/redirected')
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"ok":true,"value":{"accepted":true}}')

            def do_GET(self):
                requests.append(self.command)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok":true,"value":{"accepted":true}}')

        with tempfile.TemporaryDirectory() as temp:
            credentials = credential_file(temp)
            server = HTTPServer(('127.0.0.1', 0), Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                base = f'http://127.0.0.1:{server.server_port}'
                server.redirect = True
                with self.assertRaisesRegex(dsh_delivery.DeliveryError, 'HTTP 302'):
                    dsh_delivery.deliver(base, 'synthetic-session', 'notice', credentials)
                self.assertEqual(requests, ['POST'])
                server.redirect = False
                with patch('urllib.request.getproxies', return_value={'http': 'http://127.0.0.1:1'}), \
                     patch('urllib.request.proxy_bypass', return_value=False):
                    result = dsh_delivery.deliver(base, 'synthetic-session', 'notice', credentials)
                self.assertTrue(result['accepted'])
                self.assertEqual(requests, ['POST', 'POST'])
            finally:
                server.shutdown()
                server.server_close()
                worker.join()


if __name__ == '__main__':
    unittest.main()
