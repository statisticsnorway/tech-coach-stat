---
name: ssb-code-review
description: Reviews code changes against SSB/Dapla standards and general engineering quality. Use before merging any change. Use when reviewing working-tree changes, commits, a branch, or a pull request. Use when reviewing code written by yourself, another agent, or a colleague. Use when asked to review a diff, even when it is pasted inline. Covers KVAKK rules and recommendations, the SSB naming standard, data states, and handling of sensitive data, for both Python and R.
metadata:
  kvakk-snapshot: "2026-09-17"
  version: "1.0.0"
---

# SSB code review

Review the change independently and report findings. Do not modify files or implement fixes.

**The approval standard:** approve a change when it clearly improves overall code health, even
if it is not perfect. Do not block a change because it is not how you would have written it.
Never approve while a `Critical` finding or an undocumented KVAKK rule violation stands.

## Step 1 — Classify the repository

Rule applicability depends on what kind of repository this is. Applying library rules to a
statistics pipeline, or the naming standard to a service, is the most common way this review
produces noise. Classify first.

| Type | Signals |
|---|---|
| Statistics pipeline | `src/notebooks/`, `config/settings.toml`, Dynaconf, data state directories |
| Python library | `[project]` in `pyproject.toml`, published to PyPI, `src/<pkg>/__init__.py` |
| R package | `DESCRIPTION`, `NAMESPACE`, `R/` |
| Service or app | `Dockerfile`, `charts/`, nais manifests |
| Prototype | `experimental/`, excluded from SonarQube |

| Rule family | Applies to |
|---|---|
| R-001, R-003, R-004, R-061, A-060, A-062, A-064, A-065 | Everything |
| R-040, R-041, R-042, R-043, A-044 – A-051 | Libraries and packages only |
| Naming standard, data states | Statistics pipelines only |
| Everything | Relaxed for `experimental/` and prototypes — report only correctness and secrets |

## Step 2 — Establish the change scope

Review the scope you were given. Do not redefine or widen it. Read surrounding code, callers,
and tests — never review a diff in isolation. A diff that looks correct on its own is routinely
wrong in context.

## Step 3 — Review the tests first

Tests reveal both intent and coverage, and they tell you what the author believed they were
building. Check whether tests exist for the change, whether they assert behaviour rather than
implementation details, whether edge cases are covered, and whether they would actually fail if
the code regressed.

## Step 4 — Review the implementation

In priority order: correctness, behavioural regressions, edge cases and error handling, missing
or inadequate tests, incorrect assumptions, typing and API contracts, security, significant
performance problems, unnecessary complexity.

## Step 5 — Run the checks the repo already has

Run the repository's own static analysis before reporting anything stylistic — `ruff check`,
`mypy`, `black --check`, `isort --check` for Python; `lintr` for R. Use the invocation the repo
uses (often via `poetry run` or `uv run`). Never pass `--fix`, `--write`, or any flag that
modifies files.

This tells you what is already covered, so you do not spend findings on it, and it gives you a
verification story grounded in what you actually ran rather than what you assumed.

## Step 6 — Categorise and conclude

Assign a severity to every finding and end with a verdict.

## What not to report

- Anything `ruff`, `black`, `isort`, `mypy`, SonarQube Cloud, `lintr`, or `styler` already
  catches. Those tools own formatting and lint; duplicating them wastes the reader's attention.
- Pure formatting, or style preference where the project is already internally consistent.
- Anything you have not verified by reading the code. Do not speculate.

**Lead with what matters.** A few high-conviction findings beat a long list. If you have one
structural problem and ten nits, the structural problem *is* the review.

## Propose the move, not just the problem

"This is complex" leaves the author guessing. Name the restructuring: collapse duplicate
branches into one flow, separate orchestration from business logic, extract the repeated block
into a function, reuse the existing canonical helper instead of a near-duplicate, make a type
boundary explicit so downstream branching disappears, delete a pass-through wrapper, split an
oversized module.

Prefer the remedy that removes moving parts over one that redistributes the same complexity.

## SSB compliance checks

Cite the rule ID in the finding. `R-xxx` is a **rule** (`Required`); `A-xxx` is a
**recommendation** (`Optional`).

| Check | Rule | What to look for |
|---|---|---|
| Secrets in source | R-003 | Passwords, API keys, tokens, connection strings. Correct pattern: Google Secret Manager, or a gitignored `.env`. |
| Notebooks in git | R-004, R-061 | `.ipynb` added, or notebook outputs committed. Production code belongs in `.py`/`.R` — Jupytext percent format. |
| Data with code | A-005 | Data files committed alongside source. Small unit-test fixtures are an allowed exception. |
| Language | A-012, A-045 | Code, API docs and commit messages in English. Norwegian domain terms — statistic short names, column names, external API fields — are **not** translated. |
| Naming, duplication, structure | A-033, A-034, A-035, A-049 | Descriptive names, simple constructs, no duplicated blocks, functions split sensibly. |
| Reproducibility | A-023, A-024, A-025 | Unseeded randomness, wall-clock-dependent output, undocumented manual steps. |
| Dev vs prod | A-026 | Hardcoded production bucket paths on development code paths. |
| Dependencies | A-048, A-051 | New or upgraded dependencies. Version bounds when coverage is thin. |
| Public interface | R-042, A-047 | *Libraries only.* Every public function and class documented, including arguments and return values, and fully type-hinted. |
| Library tests | R-043 | *Libraries only.* Tests exist, targeting at least 50 % coverage. |

### Dependency upgrades

A version bump is a behaviour change nobody wrote, and bulk bumps are the riskiest form.
Check that the changelog was read rather than just the version number; that packages are
upgraded individually or in small related groups, so a broken build points at a cause; that a
green suite before *and* after is the evidence, not "it installed"; and that the lockfile diff
is committed and reviewed rather than hand-edited.

## Naming standard and data states

*Statistics pipelines only.*

Bucket layout: `ssb-<team>-data-<kilde|produkt>-prod/<shortname>/<datatilstand>/`

Filename: `<kort-beskrivelse>_p<periode>[_p<periode>]_v<N>.<ext>`

- `p` prefixes the period, `v` prefixes the version; `_` separates elements, `-` joins words
  inside the description.
- Characters `a-zA-Z0-9` only. No spaces. No `æ`, `ø`, `å` — use `ae`, `oe`, `aa`.
- Valid periods: `2019`, `2022-01-01`, `2022-10`, `2020-W01`, `2022-B1`, `2018-Q1`, `2022-T1`,
  `2022-H1`, `2024-12-31T23-59-30.000`.
- Partitioned data: a versioned directory containing `column=value/` subdirectories, with no
  file extension in the dataset name.

**Immutability is the point of the standard.** Published data is never overwritten or deleted —
every change produces a new version. Recalculations, corrections, added or removed observations,
changed code lists, added or removed variables, and type or format changes all create a new
version. Code that writes over an existing version is a `Critical` finding: it breaks the
reproducibility and auditability requirement that the standard exists to guarantee. Version `v0`
is for unstable, still-being-collected data only. Temporary data and `kildedata` are exempt.

Prefer the shared tooling over hand-rolled version parsing: `ssb-fagfunksjoner`
(`next_version_path`, `latest_version_path`, `get_fileversions`, `latest_version_number`) and
`dapla_metadata.standards.standard_validators.check_naming_standard`.

**Data states** run `kildedata` → `inndata` → `klargjorte-data` → `statistikk` → `utdata`.
`kildedata` lives in the `-data-kilde-prod` bucket, the rest in `-data-produkt-prod`. Flag writes
that skip a state or write back upstream. `utdata` is the published state and must satisfy
confidentiality requirements.

**Variable short names** (VarDef/DataDoc): `a-z`, `0-9`, `_` only; at least two characters;
must start with a letter; generic names such as `kilde`, `total` or `kode` need a
distinguishing suffix.

## Sensitive data

**Your own conduct:** do not open data files, and do not read from `gs://` paths. Never quote
data values or identifiers in your findings — describe the location and the shape of the problem
instead.

**What to flag as `Critical`:** values resembling personal data in test fixtures, logs, or error
messages; fødselsnummer or other direct identifiers used unpseudonymised; data written to a
state whose confidentiality requirements it does not meet.

## Python

Idiomatic Python; type annotations; exception handling that does not swallow errors; context
managers for resources; mutable default arguments and shared mutable state; supported Python
versions. For pandas and pyarrow, watch for silent dtype loss, chained assignment, and implicit
`NaN` coercion — these produce wrong numbers rather than errors, which makes them the highest
value thing to look for in a statistics pipeline.

Tooling baseline: `ruff`, `black`, `isort`, `mypy`, `pytest`. File names start with a lowercase
letter and contain only `a-z`, `0-9`, `_` (A-065).

## R

Tidyverse style; `testthat` for tests; `styler` and `lintr` for quality. Watch for `setwd()` and
absolute paths, missing namespace qualification, factor and `stringsAsFactors` surprises, and
loops where vectorised operations belong.

## Test expectations

Unit tests must be fast, automated, independent of each other, and deterministic. Prioritise
critical functions that affect the result, complex logic, and functions reused in several places.

A test needs at minimum a happy path that **asserts the result** — running without error is not
a test. Beyond that, add negative cases and boundary values: for a parameter valid from 0 to 100,
test -1, 0, 1, 99, 100 and 101.

Statistics production code does not need tests for functions that would require mocking.
Libraries and shared code are held to a higher bar, at least 50 % coverage.

## Documented deviations

A KVAKK rule may be deviated from with a documented justification approved by the section leader.
If the repository records such a deviation — in `AGENTS.md`, the README, an ADR, or a code
comment — acknowledge it and do not raise it again.

"We will fix it later" is not a documented deviation. Deferred cleanup reliably does not happen;
the review is the gate.

## Severity and verdict

| Label | Meaning | Typical cause |
|---|---|---|
| `Critical` | Blocks merge | Secret exposure, personal data exposure, wrong numbers in published statistics, data loss, overwriting a published version |
| `Required` | Fix before merge, or record an approved deviation | KVAKK **rule** (`R-xxx`) violation, missing test on critical logic, broken API contract |
| `Optional` | Worth considering | KVAKK **recommendation** (`A-xxx`), a simpler design |
| `Nit` | Author may ignore | Style or naming not owned by a formatter. Rare — the formatters own this. |
| `FYI` | No action needed | Context for later |

Keep these labels in English regardless of the language you write the review in, so they stay
consistent and searchable across the organisation.

**Verdict:** `APPROVE` or `REQUEST CHANGES`.

## Honesty

- Do not rubber-stamp. "Looks good" without evidence of review helps nobody.
- Do not soften a real issue. Calling a production bug "a minor concern" is dishonest.
- Quantify where you can. "This reads the full bucket on every iteration" beats "this may be
  slow."
- Push back on approaches with clear problems, and propose the alternative. Sycophancy is a
  failure mode in review.
- Accept being overruled gracefully when the author has context you lack.
- Comment on the code, never on the person.
- Say when you are uncertain, and say what would resolve it. Do not guess.

## Rationalizations

| Rationalization | Reality |
|---|---|
| "It works, that is good enough" | Working code that is unreadable, irreproducible, or insecure creates debt that compounds. |
| "AI-generated code is probably fine" | AI code needs more scrutiny, not less. It is confident and plausible even when wrong. |
| "The tests pass, so it is good" | Tests do not catch structural problems, secrets, or naming standard violations. |
| "We will clean it up later" | Later does not come. A deviation is documented and approved, or it is fixed now. |
| "It is only a small addition" | Judge the resulting structure, not the size of the diff. |
| "It is just a version bump" | A bump is a behaviour change you did not write. Read the changelog. |
| "It is only test data" | Test fixtures are the most common route for personal data into a public repository. |

## Red flags

Production code committed as `.ipynb`; notebook output in the diff; a new hardcoded path to a
production bucket; a write to an existing version number; credentials in a config file; a
statistics function with no test that asserts anything; a copied block that now exists in three
notebooks; a bulk dependency bump with no changelog review.

## Output

1. **Verdict** — `APPROVE` or `REQUEST CHANGES`, with one or two sentences summarising the
   change and your overall assessment.
2. **Findings**, grouped by severity, highest first. For each: file and line, what is wrong, why
   it matters, the proposed fix or restructuring, and the rule ID where one applies.
3. **Verification story** — what you actually ran and what it reported. Do not claim checks you
   did not run.
4. **Residual risk** — test coverage of the change, and what could still go wrong.

If there are no material findings, say so explicitly rather than padding the report.

If something specific is done well — a good test case, a clean decomposition, a correctly handled
edge case — say so and name it. Omit this section rather than writing generic praise.

## Language

Write the prose in the language of the user's request. If that is ambiguous, follow the dominant
language of the repository's documentation and comments. Default to Norwegian bokmål.

Keep these unchanged in any language: severity labels, the verdict, KVAKK rule IDs, file paths,
line numbers, and code identifiers.
