# Per-agent usage reports

`usage_report.py` is a local Python-standard-library API and CLI. It reads explicitly
selected sources and emits schema-version-1 JSON. It sends no messages, installs no
hooks, writes no Git notes, and calculates no costs. Keep manifests, markers, reports,
and native usage records outside Git. They contain private session identities.

## Selection

Create a selection for the session whose own shell runs the command:

```sh
python3 usage_report.py self --provider codex > /private/runtime/selection.json
python3 usage_report.py self --provider claude --project /your/project > /private/runtime/selection.json
```

Codex uses `CODEX_THREAD_ID` and `CODEX_HOME` (default `~/.codex`). Claude uses
`CLAUDE_CODE_SESSION_ID` and `CLAUDE_CONFIG_DIR` (default `~/.claude`). An absent or
ambiguous identity refuses selection. No replacement session is created. Claude's
optional `--include-subagents` reads only paths under the selected session's
`subagents` directory. Codex subagents require explicit source selection; it does
not inspect unrelated transcripts to find their lineage.

`participants --state-root /selected/install/state` enumerates Koinon registration
metadata only. Native Claude sessions are selected using `self`, not this registry.
No transcript is read by the inventory operation. Explicit selection never overrides
filesystem permissions or authorizes reading another user's data.

An explicit manifest has this shape (all names below are synthetic):

```json
{"schema_version":1,"agents":[{"agent_id":"agent-a","session_id":"session-a","provider":"codex","path":"/private/runtime/rollout-session-a.jsonl","role":"main"}]}
```

Native roles are checked against source lineage: Codex `session_meta.source` identifies
main/subagent sessions; Claude uses the selected main transcript or its subagent
path. A conflicting declared role produces an inconsistent report. Environment flags
such as `CLAUDE_CODE_CHILD_SESSION` do not establish role. Each result preserves agent
identity and produces one row per agent/session/model/role segment.

## Boundaries and output

For a request made before work, write a marker into an existing private runtime directory:

```sh
python3 usage_report.py begin --manifest /private/runtime/selection.json --work-block task-a --output /private/runtime/task-a.marker.json
python3 usage_report.py report --manifest /private/runtime/selection.json --marker /private/runtime/task-a.marker.json --output /private/runtime/task-a.report.json
```

The marker records source identity, a prefix checksum, and already observed response
IDs. Only later response IDs enter the report. Replaced, truncated, or rewritten
prefixes refuse the boundary. Markers and reports are created exclusively with mode
0600; existing files are never overwritten. Markers may be used repeatedly for
successive snapshots. Distinct marker files permit overlapping/nested blocks; their
counts overlap and must not be added as disjoint work. An unfinished block has only
its start marker. Reusing an output path fails rather than replacing that marker.

For completed work with known timestamps:

```sh
python3 usage_report.py report --manifest /private/runtime/selection.json --work-block task-a --since 2026-01-01T10:00:00Z --until 2026-01-01T11:00:00Z
```

The interval is `(since, until]`, using source-record timestamps converted to UTC.
Responses with an observed first row before the boundary and later updates inside
it are excluded and flagged as crossing the boundary. Records after `until` cannot
replace an earlier partial row inside the interval. These timestamps establish
observation windows, not causal attribution to a task or commit.

The measurement ends at the captured file prefix. The response running the report
command cannot include its own future usage. Unwritten responses are outside this
snapshot. Observed unfinished responses remain provisional and make the report
incomplete; interrupted responses do not cause an indefinite wait. A complete status
means the selected observed records have complete, consistent counters, not that all
work on an ongoing task has finished or every provider response was retained.

Each agent can produce its own report. Combine those files without reopening sources:

```sh
python3 usage_report.py combine /private/runtime/agent-a.json /private/runtime/agent-b.json --output /private/runtime/team.json
python3 usage_report.py table /private/runtime/team.json
```

Combine requires the same block ID and refuses duplicate agent identities across
reports. It preserves evidence, boundaries, and incomplete/inconsistent status.
The table has exactly Model, Role, Tokens In, Tokens Out, Cache Write, Cache Read,
Reasoning, Total. Agent identity is displayed separately. JSON is the primary API;
consumers must check top-level and row status, not just numeric totals.

## Counter contract and evidence

The five components are disjoint. Tokens In excludes cache reads/writes; Tokens Out
excludes reasoning. Total is stored explicitly. Native totals are retained where
available; Claude's total is derived from its native input/output and cache counters.
A complete breakdown must sum to Total. Inconsistency is reported without correcting
Total, clamping native counters, or substituting zero for missing measurements.
For partial rows, numeric columns are subtotals of complete, consistent responses
only, and `counter_coverage` states that rule. Other responses remain in
`excluded_responses`, with identity, timestamp, reasons, native values, and normalized
counters (including an inconsistent original total). The row stays incomplete or
inconsistent. If no response qualifies, the subtotal is unavailable, not zero.
The table states how many responses were counted and excluded. Synthetic Claude
API-error rows are excluded from model rows and counted separately in evidence.
Native counter aggregates, source prefix hashes, selection, and mapping version are
retained as evidence. No conversation bodies enter reports.

| Source | Mapping and completion | Evidence and limits |
| --- | --- | --- |
| Codex | Completed `token_usage_record.payload.usage`; cache buckets are subtracted from input, reasoning from output. Duplicate response IDs are not summed. | Own-session zero-cache-write observations plus the official nonzero parse fixture below support this convention. The fixture is not a live nonzero observation or a universal provider invariant. Runtime defaults can obscure an upstream omitted field. |
| Claude | Deduplicate `message.id` per selected session/source; latest observed usage wins. Recognized non-null `stop_reason` establishes observed response completion. Cache buckets are additional to input; thinking is inside output. | Independent selected-session observations include repeated identical rows, growing updates, and an interrupted response with null stop reason. Missing thinking remains unavailable. Async subagent completion is not live verified. |
| DeepSeek | Not supported in this release. | Usage reporting is explicitly deferred by scope decision. This does not change DeepSeek messaging, delivery, or installation support, and is not a claim that its counters are unavailable. |

The official [Codex parse fixture](https://github.com/openai/codex/blob/fa8cf449858c7fffc83d9e3604894852344962a1/codex-rs/codex-api/src/sse/responses.rs)
uses nonzero cache-write values consistent with inclusive input and copies the usage
fields unchanged (blob `3f867d5d0b48ca964b9ffb94e01af1cbb1583382`).
[Forge's capture adapter](https://github.com/cordalo/forge/blob/9a81559cd075a291b0640153de778b245e2b4adb/scripts/capture-opencode-usage.sh)
copies OpenCode session counters. Current [OpenCode normalization](https://github.com/anomalyco/opencode/blob/9b0dd36cda0b9accb429a7f9f9ad9b054a27d04a/packages/opencode/src/session/session.ts)
subtracts both cache buckets and reasoning, but defaults missing values to zero and
clamps negative results. Koinon does neither. This traces the current source convention;
it does not establish the version of every Forge deployment.

Only native Codex and Claude sources are accepted in this release. Generic
normalized-record import is not included. Missing keys remain unknown. Boolean,
negative, noninteger, and out-of-range counters are invalid. Sources are bounded to
1 GiB, 4 MiB per line, and 250,000 unique responses; manifests select at most 256
sources. Partial trailing lines are diagnosed. Oversized lines are consumed in bounded chunks and diagnosed while other records
remain available. Missing, unreadable, oversized source files, or
unrecoverable boundaries fail explicitly; no permission changes or automatic discovery
of unrelated sessions is attempted.

`excluded_api_error_rows` counts synthetic/API-error rows in the whole captured
source prefix, not only the selected work window. It is source evidence rather
than a response count for that window.
