# Issue tracker: GitHub

Issues and specs for this repository live in GitHub Issues. Use the `gh` CLI from this checkout so the repository is inferred from the `origin` remote.

## Conventions

- Create an issue with `gh issue create` and a heredoc body.
- Read an issue and its discussion with `gh issue view <number> --comments`.
- List issues with `gh issue list`, requesting labels and comments as structured JSON when filtering.
- Add or remove labels with `gh issue edit`.
- Comment with `gh issue comment` and close with `gh issue close`.
- Publish specs and implementation tickets with the `ready-for-agent` label once they are complete.

## Pull requests as a triage surface

**PRs as a request surface: no.** Pull requests are not treated as incoming feature requests by the triage flow.

## Native relationships

Use GitHub sub-issues for parent and child relationships and GitHub issue dependencies for blocking edges when those APIs are available. A dependency edge must use the blocker's numeric database ID, not its issue number or node ID. If the repository does not expose native dependencies, fall back to an explicit `Blocked by: #<number>` line in the issue body.
