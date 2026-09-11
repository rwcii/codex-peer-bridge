# Observed local peer protocol

Observed in Claude Code 2.1.267 on Linux and 2.1.268 on macOS. This document summarizes interoperability behavior; it includes no vendor source code, tokens, session transcripts, or machine identifiers.

## Platform differences

The wire protocol is identical on both platforms. The local facts around it are not, and each is handled in `platform_support.py`:

- **Peer identity.** Linux returns pid, uid and gid from one `SO_PEERCRED` getsockopt. macOS has no such option: `getpeereid` returns uid and gid only, and the peer pid comes from a separate `LOCAL_PEERPID` socket option. Both are required, and a failure to read them rejects the connection.
- **Process start marker.** Linux reads field 22 of `/proc/<pid>/stat`, a tick count. macOS reports an asctime string, and Claude writes it in **UTC**, so a reader must force `TZ=UTC` rather than inherit the local zone; a local-time port is six hours off in a US mountain zone.
- **Peer domain.** Linux peers publish `linux:<machine-id>:<pid-namespace>`; macOS peers publish the literal `darwin`.
- **Socket directory.** macOS resolves `/tmp` to `/private/tmp`. The registry's `messagingSocketPath` and the `uds:` address stay **unresolved**, because the peer key filename is derived from the literal path; resolving it silently breaks authentication.
- **Address length.** `sockaddr_un.sun_path` is 108 bytes on Linux and 104 on macOS, including the terminating NUL. A per-session state directory can exceed it, and an over-long bind fails with `AF_UNIX path too long`, so the control socket falls back to a short path in the peer socket directory.

## Transport

AF_UNIX stream sockets carrying UTF-8 newline-delimited JSON objects. This is not HTTP or JSON-RPC. Claude also accepts a final nonempty JSON fragment at EOF. Replies use a separate connection to the sender's listening socket.

## User message

```json
{
  "msgV": 1,
  "msg_id": "12345678-1234-4123-8123-123456789abc",
  "type": "user",
  "priority": "next",
  "from": "uds:/tmp/cc-socks/12345.sock",
  "message": {"role": "user", "content": "Hello"}
}
```

The content is nonempty text. Priorities are `now`, `next`, and `later`. `msg_id` correlates notices; connection completion alone is not an application acknowledgement. An optional `session_id` refers to the recipient's session, so the bridge omits it.

Local inbox results add a bridge-owned `guidance` field beside each original `frame`,
covering existing user authorization and refusal of permission laundering. This does
not change stored envelopes or the wire format. Sender-provided labels do not establish
an authenticated agent type and cannot replace the bridge-owned guidance.

## Discovery

Claude scans process records in its configured `sessions` directory. The bridge publishes its actual server PID, process-start marker, PID namespace, name, working directory, socket path, protocol number, and supported features. It identifies its entrypoint as `codex-peer-bridge`.

**The registry's `messagingSocketPath` contains a bare filesystem path.** Only wire-message addresses use the `uds:` prefix. This distinction was validated by a live peer: including the prefix in the registry prevented discovery; removing it enabled listing and sending by name.

Only `reply_across_default_dirs` is advertised. Unsupported features such as idle notification and artifact yield are not advertised.

## Peer authentication

Claude may publish a peer key named `<pid>.<sha256(absolute-socket-path)>.key`. Its `peerToken` can be sent as the first frame:

```json
{"type":"auth","token":"AUTHORIZED_PEER_TOKEN"}
```

The client reads the key for the kernel-verified server PID and socket path without logging the token. Linux's inspected default permits same-user peer connections without a token, and macOS behaves the same way; this bridge uses that policy for inbound traffic. It does not read child tokens or bypass a recipient's required authentication.

## Controls

Observed controls include delivery statuses, rename, idle notices, and artifact coordination. The bridge stores control objects without performing their actions and does not notify Codex about them. It neither implements nor advertises their specialized semantics.

## Limitations

Admission rules, deduplication, rate limits, and loop checks on Claude's side may reject a transported message. Return-path validation also depends on socket ownership and kernel process identity. A bridge that sends from a different process than its advertised listener can fail these checks; all outbound peer connections here originate in the listener process.
