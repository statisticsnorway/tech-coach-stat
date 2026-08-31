---
name: code-review
description: Review code changes for correctness, regressions, test gaps, typing issues, maintainability, and security. Use when reviewing working-tree changes, commits, branches, or pull requests.
---

# Code review

Review changes independently.

Focus primarily on:

- correctness and potential bugs
- behavioral regressions
- edge cases and error handling
- missing or inadequate tests
- incorrect assumptions
- typing and API-contract problems
- security issues
- significant performance problems
- unnecessary complexity

For Python, also consider:

- idiomatic Python
- type annotations
- exception handling
- resource management
- mutable state and side effects
- supported Python versions

Read surrounding code when necessary. Do not review the diff in isolation.

Report findings first, ordered by severity.

For each finding provide:

1. Severity: high, medium, or low
2. File and line
3. What is wrong
4. Why it matters
5. Suggested fix

If there are no material findings, explicitly say so.

Finish with a short assessment of test coverage and residual risks.
