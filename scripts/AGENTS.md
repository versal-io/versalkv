# scripts

## Scope

This file applies to every file under `scripts/`.

## Single Public Entry

- `scripts/versal-util.py` is the only public executable entry point in this directory.
- Makefiles, CI pipelines, Git hooks, root runners, and developer documentation must invoke repository tooling through `python3 scripts/versal-util.py ...`.
- `versal-util.py` is the only Python file allowed directly under `scripts/`; command implementations and hermetic self-tests live in that file.
- Add new repository commands as `versal-util.py` subcommands instead of creating directly executable scripts.

## Constraints

- Keep the utility compatible with Python 3.7 and the standard library.
- Keep source text in English.
- Preserve hermetic self-tests for command routing and repository policy checks.

## Verification

Run:

```bash
python3 -m py_compile scripts/versal-util.py
python3 scripts/versal-util.py selftest all
python3 scripts/versal-util.py check ci-support
```
