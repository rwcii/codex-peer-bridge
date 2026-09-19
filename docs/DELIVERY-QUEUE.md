# Delivery queue

This is the repository record of outstanding delivery work. It supplements the
[programme contract](PARITY-MEMORY-DESIGN.md); an entry is not evidence that a
feature works. Distinguish user requirements, confirmed defects, and proposals.
Each item closes only with its stated evidence. Runtime data and private handoff
content do not belong here.

## Order and ownership

The driver owns implementation and documentation. The peer reviews independently.
Confirm task acceptance before relying on peer work. Coordinate release and runtime
work separately; this queue does not authorize a merge or deployment.

Stage 4 implementation was squash-merged in PR #18. Its tests and review do not
close the items below. Check the live release and deployment records before acting;
branch completion and installed capability are different facts.

Recommended order: finish the terminology correction, define the usage-report
contract and its evidence sources, resolve the provider installation defect, then
continue Stage 5. Keep the retrieval proposals visible for a scope decision before
Stage 6 history pruning. Investigating a requirement does not settle its protocol.

## DQ-01 — Universal per-agent usage reports

**Source:** user requirement conveyed by the reviewer and explicitly requested for
this queue by the user. **Status:** queued; report contract and acquisition design
required before implementation.

An agent reports its own model and token usage for a specified work block on
request. The request may be made at the start of the block or after it completes.
Preserve each reporting agent's identity, with model/role segments where needed,
and these columns:

| Model | Role | Tokens In | Tokens Out | Cache Write | Cache Read | Reasoning | Total |
| --- | --- | --- | --- | --- | --- | --- | --- |

The user clarified through the reviewer that Role means `main` or `subagent`
(harness position). Driver and reviewer describe a participant's task or function,
not Role. Keep any such assignment as metadata, not a ninth displayed column.
Preserve agent identity alongside model and role so distinct agents never collapse
into one row merely because their model and harness role match.

The deliverable is usage data available to agents universally: acquisition,
normalization, hazard handling, and a report for a requested work block. Participant
session enumeration is in scope so relevant records can be identified. Enumeration
must preserve explicit participant selection and existing permissions; it is not
permission to read every session on the host.

Commit notes are one consumer of this report, not a Koinon repository-tooling
pipeline. Writing notes, pushing notes refs, capture hooks, merge aggregation,
pricing tables, cost derivation, and cost reports are out of scope. The downstream
commit note and the cost/provenance note produced by a merge-time action are separate
records. Existing user tooling adapts to the agent data interface, not the reverse. The user specifically named the provenance-notes skills in
`cordalo/forge` and `cordalo/unify-messaging`; inspect those skills and their supporting
implementation before choosing metric sources. The driver has read the
unify-messaging provenance skill and parser, and the Forge notes script. Their
end-to-end capture behavior has not been tested here.

Acceptance requirements:

- Support a request made before work and a retrospective request for completed work.
- Identify the reporting agent and work block unambiguously; retain the eight
  requested columns and one row per reporting agent.
- Store Total as a field in the report for automation; do not leave it solely as a
  display-time calculation.
- Include only the metrics required for the commit notes. The eight requested columns
  are the complete report set for this requirement; additional metrics and cost
  estimation are separate scope.

Proposed correctness gates, to resolve in the implementation contract:

- A request at work start records the measurement boundary; it does not predict
  future usage. Define how a retrospectively requested block is identified.
- Define each provider's field meanings and whether cached or reasoning tokens
  overlap its input/output totals. Do not sum overlapping categories twice.
- Inspect the named references and record a provider-by-provider availability matrix
  for all requested counters. No specific counter has yet been established as
  unavailable; an uninvestigated source is not an unsupported capability.
- Use authoritative available measurements. Any unavailable value needs a specific
  evidenced reason, such as an unexposed counter or an unrecoverable work boundary.
  Do not substitute zero, infer another agent's usage, or invent historical data.
  Report incomplete coverage explicitly; do not count it as full implementation.
- State the source and coverage of a report and define model changes within a block.
- For a complete normalized breakdown, verify stored Total against its components.
  Report a mismatch as inconsistent data; do not silently rewrite either the stored
  total or its components. A partial breakdown cannot establish that equality.
- Test before-work and retrospective requests, missing historical measurements,
  multiple agents, partial reports, and prevention of double counting.

Acquisition evidence from the design conference:

- The current Codex runtime writes `token_usage_record` entries to its own session
  JSONL record. `payload.usage` contains `input_tokens`, `output_tokens`,
  `cache_write_input_tokens`, `cached_input_tokens`, `reasoning_output_tokens`, and
  `total_tokens`. Model identity is present in `turn_context.payload.model`; role
  means harness position (`main` or `subagent`), established from session lineage
  rather than a token counter. The driver read its own records and verified
  their session attribution, response identifiers, and all six fields.
- In that inspected sample, per-response sums matched the thread aggregate, total
  equalled input plus output, and cache reads and reasoning were bounded by input
  and output respectively. All observed cache-write counts were zero, so the sample
  does not establish nonzero cache-write accounting. Do not generalize this evidence
  to every provider or runtime version.
- Some separate `token_count` event snapshots had a nonzero total with zero component
  fields. They are not interchangeable with per-response usage records. A collector
  must distinguish event kinds and avoid summing cumulative snapshots or duplicate
  responses. A session total is not a work-block report without identified boundaries.
- The reviewer independently reports a Claude session-record source. Its provider
  mapping and aggregation require their own checks; copying the Codex total formula
  or adding cache fields to Codex input would be incorrect without normalization.
- Keep only field names and sanitized findings in repository documentation. Private
  session paths, identifiers, contents, and actual runtime counts stay outside it.

- The reviewer found repeated Claude message IDs carrying growing streaming usage
  updates and withdrew its earlier sum of all usage rows. Deduplicate within the
  selected provider/session/response identity. The last observed update is not
  necessarily final during an active response; establish completion or label the
  report provisional and define how later updates replace it.

Reference reconciliation still required:

- Unify-messaging's `.claude/skills/provenance/SKILL.md` and
  `scripts/provenance-parse.sh` use local transcripts and out-of-band Git notes.
  Commit-time windows are defaults; explicit time windows and post-commit review-tail
  capture also exist. These defaults do not replace arbitrary requested work blocks.
- Its model/main-or-subagent aggregation is distinct from the requested per-reporting-
  agent presentation. Preserve the confirmed harness-role meaning and model changes without losing agent
  identity or silently changing the user's requested granularity.
- Forge's `scripts/provenance-notes.sh` includes a reasoning counter and an accounting
  expression that adds output and reasoning. Trace its upstream normalization before
  applying it to Codex, whose inspected runtime output already includes reasoning.
  The references are not one interchangeable schema. Cost remains out of scope.
- Local collection with explicit session selection is the current recommendation,
  not an implemented feature. Git notes are an example downstream consumer, not
  the destination this project must implement. Same-host peer transport does
  not prove that every transcript is retained, mounted, or readable from a collector's
  sandbox. Preserve existing permissions and never scan unrelated sessions by default.
- Best-effort capture does not waive validation of local records or permit missing
  values to become measured zeros. Report coverage separately from valid counts.
- Recommend disjoint ordinary-input, cache-read, and cache-write categories for an
  explicit report contract, retaining native measurements for reconciliation.
  Codex subtraction is checked only for observed zero-cache-write records; nonzero
  cache-write inclusion and category disjointness still need evidence. Inclusive input
  with separately retained cache subsets is also reversible when their relationships
  are known; the chosen report contract is not dictated by a reference file layout.

Recommended report convention, pending design acceptance: make the five numeric
components disjoint. Tokens In means ordinary input excluding cache reads and writes;
Tokens Out means output excluding reasoning. Store Total explicitly as Tokens In + Tokens Out +
Cache Write + Cache Read + Reasoning when the normalized breakdown is complete. Keep native counters and the mapping version
as acquisition evidence. The proposed partitions reconcile for the inspected samples;
nonzero Codex cache-write semantics still require verification. Missing components
are not zeros and cannot silently produce a complete normalized breakdown. A measured
native total can remain available even when its breakdown is incomplete. This is a
reporting convention, not a change to provider-native counter meanings.

**Implementation design remaining:** define the agent-facing request/report interface,
participant enumeration, work-block boundaries, source adapters, and completeness
rules. Local collection is the recommended acquisition path for the measured sources.
No new wire format, hooks, note-writing pipeline, or downstream cost tooling is
implied by this queue. Keep the report independent of Forge's file layout and call
conventions; existing consumers will adapt to this interface.

## DQ-02 — DeepSeek-only installation must not require Codex

**Source:** recorded implementation defect (F071). **Status:** correction implemented;
review and CI pending. DeepSeek-only setup and repeat installation no longer require
Codex. Codex startup refuses a missing executable before creating new session state.

The installer validates the Codex executable unconditionally before it resolves the
installation mode and participant set. On a DeepSeek-only host without Codex on PATH,
that check fails before provider-specific configuration is selected. This contradicts
the documented provider requirement. Resolve the mode and participant set before
validating their required executables; a check inside a later DeepSeek branch would
leave the earlier unconditional failure in place.

Acceptance: demonstrate the failure with a synthetic DeepSeek-only setup and no
Codex executable; correct provider-specific validation; verify DeepSeek-only setup
and repeat installation succeed while Codex and mixed-participant setup still
validate their required executable. Preserve saved paths, targets, and state.

## DQ-03 — Stage 5 presence, priority, and delivery evidence

**Source:** approved programme contract. **Status:** pending implementation.

Separate fresh, evidenced model activity from service health. Unknown activity must
remain unknown. Declare provider priority limits from measurement. Distinguish
transport, stored, notified, fetched, and explicitly handled outcomes. Inbox
acknowledgement is deletion, not proof of handling.

Acceptance follows Contracts 2 and 3. Include durable deduplication that survives
inbox acknowledgement, bounded retry deadlines, payload-identity conflicts, and
uncertain outcomes. Receipts must neither wake models nor form receipt loops.
Use native controls only after their semantics are verified.

Open capability checks include native idle and receipt frames, accepted registry
activity values, a read-only source owned by the actual participant, relevant hook
events, and native deduplication and priority scheduling. Use isolated consenting
fixtures; never advertise production support from an assumption.

Design provider result classification, supervisor exits, ownership, schema migration,
and resource limits together before implementing handlers. Reuse the existing
bounded transport and database workers. Local preparation notes are not completion
evidence or a substitute for this contract.

## DQ-04 — Selective memory retrieval

**Source:** gap discussed with the user; the proposed solution below is not an
approved implementation contract. **Status:** scope/design decision pending.

Records carry type, scope, scope target, and optional path. Current `recall` uses FTS
token matching when the index is usable and a body-substring scan otherwise, and
paginates newest-first; it has no metadata filters or first-class topic
model. Initial sync selects repository records and later sync returns repository
changes; stored scope is not a server-side audience filter. Typed storage alone does
not give agents selective retrieval or scope isolation.

Proposal: define type, scope/target, path, and topic selection, including combined
filters and metadata-only queries. Specify topic identity/indexing rather than treating
a body keyword as a topic. Define applicability for standing directives. Decide
separately whether sync needs consumer-specific selection; do not add filters that
silently skip data through a shared acknowledgement cursor.

Proposed acceptance: stable pagination, matching filter behavior on indexed and scan
paths, explicit query semantics, migration of existing records, and tests proving
scope and cursor behavior. Retention by memory class is a proposal requiring a stated
policy, not permission to expire existing decisions.

## DQ-05 — Stage 6 claims and history pruning

**Source:** approved programme contract with corrected terminology. **Status:** pending.

Claims are advisory leases with explicit renewal, ownership generation, and
prefix-overlap conflict detection. They do not fence filesystem writes.
History pruning removes history no longer required by policy; it is not semantic
summarization. Preserve live directives, conflicts, recovery information, and bounded
storage. Verify snapshot pagination during pruning, expired snapshots, and replay
after a crash before acknowledgement. Complete the remaining content checks in the
programme contract.

Semantic memory consolidation has no accepted design or implementation in this
programme. It must not be counted as delivered by expiry, pruning, or SQLite vacuum.
See [memory maintenance terminology](PARITY-MEMORY-DESIGN.md#memory-maintenance-terminology).

## DQ-06 — Repository housekeeping

**Source:** reviewer proposal. **Status:** inventory and coordination required.

List merged branches and worktrees, their owners, and uncommitted work. Propose
cleanup only after confirming that no agent needs them. Preserve active branches,
local edits, and private handoff files. Do not infer deletion permission from a merge.

## DQ-07 — Terminology and reporting correction

**Source:** direct user request. **Status:** documentation edits prepared for review.

Use garbage collection, history pruning, storage reclamation, retained-history floor,
and semantic memory consolidation precisely. Snapshots contain records, not generated
summaries. Update the README, protocol explanation, design build order, and changelog.
No runtime or retention-policy change is part of this item.

Process correction: the driver's PR status report became stale after another session
completed the merge. Verify remote state before reporting or planning release work.
Keep outstanding commitments in this queue rather than only in temporary files or
ignored handoffs. Record both findings and their dispositions.
