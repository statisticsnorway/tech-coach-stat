---
description: Independently review current changes or changes against a base ref
---

Delegate the code review to the `reviewer` subagent.

The optional base Git ref is:

$1

Determine the review scope as follows.

## No base ref

If no base ref was provided, review all current uncommitted changes.

The reviewer should:

- Use `git status` to identify changed and untracked files.
- Review changes to tracked files with `git diff HEAD`.
- Inspect relevant untracked files reported by `git status`.
- Read surrounding code, tests, callers, and related implementation when
  necessary to understand the changes.

## Base ref provided

If a base ref was provided, review the changes introduced by the current
branch since it diverged from that ref.

The reviewer should:

- Use three-dot diff semantics equivalent to:
  `git diff <base>...HEAD`
- Treat `$1` as the base ref exactly as supplied.
- Inspect the changed files and relevant surrounding code.
- Also inspect `git status` for staged, unstaged, or untracked changes that
  are not included in `<base>...HEAD`.
- Report those additional uncommitted changes separately.

Do not modify files or implement fixes.

Return the reviewer subagent's findings to the current primary agent.
