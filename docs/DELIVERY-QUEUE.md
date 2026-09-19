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

## DQ-01 — Per-agent usage reports for commit provenance

**Source:** user requirement conveyed by the reviewer and explicitly requested for
this queue by the user. **Status:** queued; report contract and acquisition design
required before implementation.

An agent reports its own model and token usage for a specified work block on
request. The request may be made at the start of the block or after it completes.
Return one row per reporting agent, with these columns:

| Model | Role | Tokens In | Tokens Out | Cache Write | Cache Read | Reasoning | Total |
| --- | --- | --- | --- | --- | --- | --- | --- |

The figures support commit provenance notes. Cost estimation is a separate work
product and is excluded. Reference implementations named by the user are
`cordalo/forge` and `cordalo/unify-messaging`; inspect their relevant implementation
before choosing metric sources. Their behavior has not yet been verified here.

Acceptance requirements:

- Support a request made before work and a retrospective request for completed work.
- Identify the reporting agent and work block unambiguously; retain the eight
  requested columns and one row per reporting agent.
- Produce metrics suitable for commit provenance without adding cost figures.

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
- Test before-work and retrospective requests, missing historical measurements,
  multiple agents, partial reports, and prevention of double counting.

**Open design decision:** reporting convention versus bridge protocol support.
No wire-format change is approved by this queue. First inspect the reference
implementations and establish the report/data-source contract, then recommend the
smallest complete integration supported by the evidence. Do not build a costing
system or a new transport merely to populate commit notes.

## DQ-02 — DeepSeek-only installation must not require Codex

**Source:** recorded implementation defect (F071). **Status:** queued for reproduction
and correction; not claimed fixed.

The installer currently applies a Codex executable requirement on the DeepSeek-only
configuration path, contrary to the documented provider requirement.

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

Records carry type, scope, scope target, and optional path. Current `recall` searches
body text and paginates newest-first; it has no metadata filters or first-class topic
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
