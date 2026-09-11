#!/usr/bin/env bash
# Apply the personal-repository branch and merge policy. Does not change visibility.
set -euo pipefail
repo="${1:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
git config core.hooksPath .githooks
gh api --method PATCH "repos/$repo" \
  -F allow_squash_merge=true -F allow_merge_commit=true -F allow_rebase_merge=false \
  -F delete_branch_on_merge=true \
  -f squash_merge_commit_title=PR_TITLE -f squash_merge_commit_message=PR_BODY \
  -f merge_commit_title=PR_TITLE -f merge_commit_message=PR_BODY >/dev/null

# Update our named rulesets in place; never delete another rule or weaken protection
# temporarily. Required checks match the Python matrix in tests.yml.
for branch in develop main; do
  method=squash
  if [[ "$branch" == main ]]; then method=merge; fi
  payload="$(python3 - "$branch" "$method" <<'PY'
import json, sys
branch, method = sys.argv[1:]
print(json.dumps({
    'name': f'{branch} branch policy', 'target': 'branch', 'enforcement': 'active',
    'conditions': {'ref_name': {'include': [f'refs/heads/{branch}'], 'exclude': []}},
    'rules': [
        {'type': 'pull_request', 'parameters': {
            'required_approving_review_count': 0,
            'dismiss_stale_reviews_on_push': True, 'require_code_owner_review': False,
            'require_last_push_approval': False, 'required_review_thread_resolution': True,
            'allowed_merge_methods': [method]}},
        {'type': 'required_status_checks', 'parameters': {
            'strict_required_status_checks_policy': True,
            'required_status_checks': [{'context': f'test ({v})'} for v in ('3.11','3.12','3.13')]}},
        {'type': 'non_fast_forward'}, {'type': 'deletion'}
    ]
}))
PY
)"
  if ! existing="$(gh api "repos/$repo/rulesets" 2>&1)"; then
    echo "Cannot read rulesets; merge settings and local hook applied: $existing" >&2
    exit 1
  fi
  id="$(python3 -c 'import json,sys; name=sys.argv[1]; print(next((str(r["id"]) for r in json.load(sys.stdin) if r["name"]==name), ""))' "$branch branch policy" <<< "$existing")"
  endpoint="repos/$repo/rulesets"
  verb=POST
  if [[ -n "$id" ]]; then endpoint="$endpoint/$id"; verb=PUT; fi
  if ! result="$(gh api --method "$verb" "$endpoint" --input - <<< "$payload" 2>&1)"; then
    echo "Ruleset configuration failed; check account support and permissions: $result" >&2
    exit 1
  fi
  echo "$branch: PR + $method, Python checks, no force-push or deletion"
done
