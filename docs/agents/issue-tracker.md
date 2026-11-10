# Issue tracker: local Markdown files

Specs and tickets for this repo live as Markdown files under `docs/specs/`, committed to git alongside the code. Do not use GitHub Issues or the `gh` CLI.

## Layout

```
docs/specs/<feature-slug>/
├── spec.md
└── tickets/
    ├── 01-<slug>.md
    └── 02-<slug>.md
```

`<feature-slug>` is short kebab-case, e.g. `klargjorte-data`. Tickets are numbered from `01` in dependency order (blockers first). This layout overrides any default path a skill suggests (e.g. `.scratch/`).

Every `spec.md` and ticket file starts with YAML frontmatter:

```yaml
---
status: open        # open | done | wontfix
labels: [ready-for-agent]
---
```

## Conventions

- **Create a spec**: write `docs/specs/<feature-slug>/spec.md`.
- **Create a ticket**: write `docs/specs/<feature-slug>/tickets/<NN>-<slug>.md`, one ticket per file.
- **Read a spec or ticket**: read the file.
- **List tickets**: list `docs/specs/<feature-slug>/tickets/` and read the frontmatter of each file. Filter on `status` and `labels`.
- **Parent / child**: tickets belong to the spec in the same feature folder. Put `Part of: ../spec.md` near the top of each ticket.
- **Blocking**: a `**Blocked by:** 01, 02` line near the top of the ticket body, or `None (can start immediately)`. A ticket is unblocked when every blocker has `status: done`.
- **Comment**: append a dated entry under a `## Log` section at the end of the file.
- **Apply / remove labels**: edit the `labels` list in the frontmatter.
- **Close**: add a `## Log` entry explaining the outcome, then set `status: done` (or `wontfix`).

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill says "publish to the issue tracker"

Write the spec or ticket file as described under **Conventions**.

## When a skill says "fetch the relevant ticket"

Read the ticket file. Also read `spec.md` in the same feature folder.

## Wayfinding operations

Used by `/wayfinder`. The **map** is `docs/specs/<feature-slug>/map.md`; its **children** are the ticket files in `tickets/`.

- **Map**: `map.md` with `labels: [wayfinder:map]`, holding the Notes / Decisions-so-far / Fog body.
- **Child ticket**: a ticket file with `Part of: ../map.md` and `labels: [wayfinder:<type>]` (`research`/`prototype`/`grilling`/`task`). Add an `assignee:` frontmatter field when claimed.
- **Blocking**: see **Blocking** under Conventions.
- **Frontier query**: tickets with `status: open`, no `assignee`, and every blocker `done`; lowest number wins.
- **Claim**: set `assignee: <name>` in the frontmatter, the session's first write.
- **Resolve**: add a `## Log` entry with the answer, set `status: done`, then append a context pointer (gist + link) to the map's Decisions-so-far.
