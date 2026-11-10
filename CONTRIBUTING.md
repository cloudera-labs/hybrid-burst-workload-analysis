# Contributing to hybrid-burst-workload-analysis

Thank you for your interest in this project. This page tells you how to make a
change and how to send it to us.

## Before you start

Open an issue, or find one that exists, and tell us what you want to change. Do
this before you start a large change. Then we can agree on the approach before
you spend time on it.

## Make the change

1. Fork the repository and make a branch from `main`. Use a prefix for the
   branch name:

   | Prefix | Use it for | Example |
   |--------|------------|--------|
   | `feature/` | A new feature, or a change to the code or the documents | `feature/impala-memory-summary` |
   | `fix/` | A bug fix | `fix/ranking-empty-window` |

2. Install the dependencies:

   ```bash
   make install
   ```

3. Make your change. Add or update the tests for it.

4. Run the tests. They need no cluster and no `.env` file:

   ```bash
   make test
   ```

5. Read the rules in [CLAUDE.md](CLAUDE.md). They apply to people and to coding
   agents. For example: use only GET requests against the Cloudera Manager API,
   never invent a value that the source does not give, and never redefine an
   existing output column.

## Run pre-commit

We use [pre-commit](https://pre-commit.com/) to check each change. The checks
include the license header. Each Python file, YAML file and the `Makefile` must
start with the Apache License 2.0 header.

Install pre-commit and the git hook once:

```bash
pip install pre-commit
pre-commit install
```

Run all the checks before you open a pull request:

```bash
pre-commit run --all-files
```

If a file has no license header, add it with
[addlicense](https://github.com/google/addlicense):

```bash
addlicense -f .github/license-header.txt path/to/new_file.py
```

## Sign your commits (DCO)

Each commit must have a sign-off line. The sign-off certifies that you wrote the
change, or that you have the right to submit it under the open source license of
this project. This is the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
660 York Street, Suite 102,
San Francisco, CA 94110 USA

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

To agree, add this line at the end of each commit message, with your real name
and the email address of the commit author:

```
Signed-off-by: Jane Doe <jane.doe@example.com>
```

Git adds the line for you when you use the `-s` flag:

```bash
git commit -s -m "Describe the change"
```

To add a sign-off to the last commit, run `git commit --amend -s --no-edit`. To
add it to all the commits of a branch, run `git rebase --signoff main`, then push
with `git push --force-with-lease`.

A DCO check runs on each pull request. We do not merge a pull request that has a
commit without a valid sign-off.

## Open a pull request

Open the pull request against `main`. Tell us which issue it fixes. The tests,
the pre-commit checks and the DCO check must pass. A member of the project team
reviews each pull request.

## Questions

Open a [GitHub issue](https://github.com/cloudera-labs/hybrid-burst-workload-analysis/issues).
