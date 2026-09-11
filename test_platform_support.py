import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import platform_support


class StartMarkerTests(unittest.TestCase):
    def test_self_start_marker_matches_itself(self):
        marker = platform_support.proc_start(os.getpid())
        self.assertTrue(marker)
        self.assertTrue(platform_support.same_process(marker, platform_support.proc_start(os.getpid())))

    def test_missing_marker_defers_to_the_pid_check(self):
        # Older registry records carry no marker; the pid check alone decides.
        self.assertTrue(platform_support.same_process(None, 'anything'))

    def test_padding_is_absorbed_but_fields_must_match(self):
        self.assertTrue(platform_support.same_process('Fri Sep  3 17:12:17 2026', 'Fri Sep 3 17:12:17 2026'))
        self.assertFalse(platform_support.same_process('Fri Sep  3 17:12:17 2026', 'Fri Sep  3 17:12:18 2026'))

    def test_whitespace_normalization_leaves_tick_counts_alone(self):
        self.assertEqual(platform_support.normalize_start('12345678'), '12345678')
        self.assertFalse(platform_support.same_process('12345678', '12345679'))

    def test_a_long_lived_process_keeps_a_stable_marker(self):
        first = platform_support.proc_start(os.getpid())
        time.sleep(0.05)
        self.assertEqual(first, platform_support.proc_start(os.getpid()))


class ProcessTests(unittest.TestCase):
    def test_self_is_alive_and_reaped_child_is_not(self):
        self.assertTrue(platform_support.process_alive(os.getpid()))
        child = os.fork()
        if child == 0:
            os._exit(0)
        os.waitpid(child, 0)
        self.assertFalse(platform_support.process_alive(child))

    def test_a_vanished_process_raises_process_lookup_on_both_platforms(self):
        # Callers such as the notifier's bridge-liveness check catch exactly this,
        # so a stopped bridge must break their loop cleanly rather than raise a
        # platform-specific error out of the notifier.
        child = os.fork()
        if child == 0:
            os._exit(0)
        os.waitpid(child, 0)
        with self.assertRaises(ProcessLookupError):
            platform_support.proc_start(child)

    def test_pid_domain_names_this_platform(self):
        if platform_support.DARWIN:
            self.assertEqual(platform_support.pid_domain(), 'darwin')
        else:
            self.assertTrue(platform_support.pid_domain().startswith('linux:'))


class PeerCredentialTests(unittest.TestCase):
    def test_peer_pid_identifies_the_connecting_process(self):
        # The kernel reports the caller's pid; a same-process connection proves
        # the mechanism works without involving any other agent.
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp) / 'peer.sock')
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            try:
                client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                thread = threading.Thread(target=client.connect, args=(path,))
                thread.start()
                connection, _ = server.accept()
                thread.join()
                try:
                    self.assertEqual(platform_support.peer_pid(connection), os.getpid())
                finally:
                    connection.close()
                    client.close()
            finally:
                server.close()


class SocketPolicyTests(unittest.TestCase):
    def test_peer_socket_directory_is_allowlisted(self):
        allowed = platform_support.allowed_socket_dirs()
        self.assertIn(Path('/tmp/cc-socks').resolve(), allowed)

    def test_control_socket_stays_within_the_kernel_limit(self):
        limit = platform_support.SUN_PATH_BYTES[platform_support.DARWIN] - 1
        with tempfile.TemporaryDirectory() as temp:
            shallow = platform_support.control_socket_path(Path(temp))
            self.assertEqual(shallow, Path(temp) / 'control.sock')
            deep = platform_support.control_socket_path(Path(temp) / ('x' * 120))
            self.assertLess(len(str(shallow)), limit)
            self.assertLess(len(str(deep)), limit)
            # The fallback must stay in the private, user-owned socket directory.
            self.assertEqual(deep.parent.resolve(), Path('/tmp/cc-socks').resolve())
            self.assertTrue(deep.name.endswith('-control.sock'))

    def test_unicode_control_socket_paths_use_the_byte_limit(self):
        # 63 characters but 108 UTF-8 bytes: too long on Linux and macOS.
        root = Path('/tmp') / ('é' * 45)
        direct = root / 'control.sock'
        self.assertLess(len(str(direct)), 104)
        self.assertGreaterEqual(len(os.fsencode(direct)), 108)
        for darwin in (False, True):
            with self.subTest(darwin=darwin), patch.object(platform_support, 'DARWIN', darwin):
                chosen = platform_support.control_socket_path(root)
                self.assertNotEqual(chosen, direct)
                self.assertLess(len(os.fsencode(chosen)),
                                platform_support.SUN_PATH_BYTES[darwin])

    def test_control_socket_fallback_is_unique_per_state_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            first = platform_support.control_socket_path(Path(temp) / ('a' * 120))
            second = platform_support.control_socket_path(Path(temp) / ('b' * 120))
            self.assertNotEqual(first, second)

    def test_sun_path_constant_matches_the_kernel(self):
        # Checked against the kernel with literal lengths rather than derived from
        # the module's own constant, so a wrong value or an off-by-one is caught
        # instead of being restated. macOS refuses 104 characters, Linux accepts
        # 107, both including the terminating NUL.
        usable = 103 if platform_support.DARWIN else 107
        self.assertEqual(platform_support.SUN_PATH_BYTES[platform_support.DARWIN], usable + 1)
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp).resolve()
            prefix = len(str(base)) + 1

            def at(length):
                return base / ('s' * (length - prefix))

            short, long = at(usable), at(usable + 1)
            self.assertEqual(len(str(short)), usable)
            self.assertEqual(len(str(long)), usable + 1)
            first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                first.bind(str(short))
                with self.assertRaises(OSError):
                    second.bind(str(long))
            finally:
                first.close()
                second.close()

class ExistingSocketTests(unittest.TestCase):
    """A pre-existing socket is never removed on either route.

    Probing cannot distinguish a dead owner from a live listener with a saturated
    accept queue: macOS refuses a connection in both cases. Recovery for a leftover
    is therefore manual, after verifying the owner is dead, as the project already
    requires for stale peer sockets.
    """

    def test_a_pre_existing_socket_is_never_removed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'existing.sock'
            live = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            live.bind(str(path))
            live.listen(16)
            os.chmod(path, 0o600)
            before = path.lstat().st_ino
            other = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                with self.assertRaises(OSError):
                    other.bind(str(path))
            finally:
                other.close()
                live.close()
            self.assertTrue(path.exists())
            self.assertEqual(path.lstat().st_ino, before,
                             'a pre-existing socket file must not be replaced')

    def test_probing_cannot_tell_a_saturated_listener_from_a_dead_owner(self):
        # The claim that retired the reclaim helper: a failed connect is not proof that
        # nobody is listening. A live listener with a full accept queue and a dead
        # owner must therefore produce the same probed result on Darwin.
        def probe(path):
            probe_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe_sock.settimeout(1)
            try:
                probe_sock.connect(str(path))
                return 'connected'
            except OSError as exc:
                return type(exc).__name__
            finally:
                probe_sock.close()

        with tempfile.TemporaryDirectory() as temp:
            live_path = Path(temp) / 'live.sock'
            live = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            live.bind(str(live_path))
            live.listen(1)
            os.chmod(live_path, 0o600)
            queued = []
            try:
                for _ in range(64):
                    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    client.settimeout(0.05)
                    try:
                        client.connect(str(live_path))
                    except OSError:
                        client.close()
                        continue
                    queued.append(client)
                saturated = probe(live_path)
                self.assertTrue(path_exists := live_path.exists())
            finally:
                for client in queued:
                    client.close()
                live.close()

            dead_path = Path(temp) / 'dead.sock'
            dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            dead.bind(str(dead_path))
            dead.listen(1)
            os.chmod(dead_path, 0o600)
            dead.close()
            dead_result = probe(dead_path)

        if saturated == 'connected':
            self.skipTest('could not saturate the accept queue on this run')
        if platform_support.DARWIN:
            self.assertEqual(saturated, dead_result,
                             'Darwin refuses a saturated live listener exactly as a dead owner')
        else:
            self.assertNotEqual(saturated, 'connected')

    def test_socket_mode_policy(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'probe.sock'
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(path))
            try:
                os.chmod(path, 0o600)
                self.assertTrue(platform_support.socket_mode_ok(path.lstat()))
                os.chmod(path, 0o666)
                self.assertFalse(platform_support.socket_mode_ok(path.lstat()))
            finally:
                server.close()


if __name__ == '__main__':
    unittest.main()
