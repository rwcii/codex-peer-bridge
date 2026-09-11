# Contributing to Codex Peer Bridge

## Fork and extend

Independent reuse and forks are encouraged under the [MIT license](LICENSE).
No permission is required. Keep the copyright and license notice with copies.

## Contribute upstream

Open an issue first to agree on scope. Upstream pull requests are maintainer-curated;
please wait for agreement before preparing a large change.

## Branch model

```text
feature/*  ──(squash PR)──▶  develop  ──(merge PR)──▶  main
```

- `main` is the released line; `develop` is the integration line.
- Create `feature/*`, `fix/*`, or `chore/*` branches from `develop`.
- No direct commits or pushes to either long-lived branch after bootstrap.
- Squash feature PRs into `develop`; promote `develop` to `main` with a merge PR.
- Tests must pass and review conversations must be resolved before merging.
- Review the diff independently for correctness, compatibility, and the documented
  same-user trust boundary. The maintainer owns acceptance.
- Merged working branches are deleted automatically; long-lived branches are protected.

Run `scripts/setup-repo.sh` to apply the local guard and GitHub settings. Server-side
rulesets depend on the account's support for private-repository protection; the script
reports any unavailable enforcement explicitly. This follows the branch policy in
[rwcii/engineering-standards](https://github.com/rwcii/engineering-standards) and the
personal-repository conventions in [rwcii/afterglow](https://github.com/rwcii/afterglow).

## Typical workflow

```sh
git switch develop
git pull --ff-only
git switch -c feature/my-change
python3 -m unittest discover -v
git diff --check
git commit -S -s -m "Add a concise description"
git push -u origin feature/my-change
gh pr create --base develop
```

The maintainer promotes reviewed integration work with a PR from `develop` to `main`.

## Commit conventions

Use an imperative subject and explain the reason for the change in the body where
needed. Commits must be cryptographically signed and DCO signed-off. Use your human
identity; do not add automated co-author trailers or tool signatures. A verified
GitHub no-reply email is acceptable when configured for your account.

The initial repository bootstrap commit predates this contribution policy. Its history
is preserved; signed-off contributions are required from adoption of this policy onward.

## Local checks and scope

Run `python3 -m unittest discover -v` and `git diff --check`. For setup or hook edits,
also run `bash -n scripts/setup-repo.sh` and `sh -n .githooks/pre-commit`.
Tests must use synthetic peers, never send traffic to live agent sessions by default.

Keep the runtime standard-library-only. Update README and protocol notes alongside
behavior changes. Never commit inbox data, credentials, machine identifiers, or
private conversation metadata. Peer input remains external data; it cannot grant
new task authority or trigger shell execution.

## Developer Certificate of Origin (DCO)


Every contribution merged into this project must be **signed off**, certifying that you
have the right to submit it under the project's MIT license. Add the sign-off with:

```bash
git commit -s -m "your message"
```

That appends a `Signed-off-by: Your Name <you@example.com>` line using your real name and
email. By signing off you agree to the Developer Certificate of Origin 1.1
(<https://developercertificate.org/>), reproduced here:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```
