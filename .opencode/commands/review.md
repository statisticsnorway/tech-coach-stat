---
description: Independently review current changes or changes against a base ref
---

Delegate the code review to the `reviewer` subagent.

The optional base Git ref is:

$1

## Argument Parsing & Scope Identification

1. **Check the base ref argument above**:
   - If the line under "The optional base Git ref is" is empty, blank, or contains the literal text "$1", then **no base ref was provided**.
   - Otherwise, the provided base ref is the exact string supplied.

2. **If no base ref was provided**:
   - Run `git status` and `git diff HEAD` to check for uncommitted changes (tracked, untracked, staged, or unstaged).
   - **If uncommitted changes exist**: Review these uncommitted changes.
   - **If the working tree is clean (no uncommitted changes)**:
     - Automatically determine a base branch to compare against. Check for an upstream tracking branch using `git rev-parse --abbrev-ref --symbolic-full-name @{u}` or check common default branches (e.g., `main`, `master`, or `develop`).
     - Find the common merge base using `git merge-base <detected_base> HEAD`.
     - Review the committed changes on the current branch since that merge base.
     - If no divergence or base branch can be determined, report that no changes were found to review.

3. **If a base ref was provided**:
   - Review the changes introduced by the current branch since it diverged from that ref.
   - Use three-dot diff semantics equivalent to:
     `git diff <base>...HEAD`
   - Inspect the changed files and relevant surrounding code.
   - Also inspect `git status` for staged, unstaged, or untracked changes that are not included in `<base>...HEAD`, and report those separately.

## Review Guidelines

- Do not modify files or implement fixes.
- Focus on code correctness, regressions, test coverage, and project-specific patterns.
- Return the reviewer subagent's findings to the current primary agent.
