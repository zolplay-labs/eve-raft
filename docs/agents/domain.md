# Domain docs

This repository uses a single domain context.

## Before exploring

- Read `CONTEXT.md` for the canonical domain language.
- Read the relevant records under `docs/adr/` before changing an architectural boundary.
- If either location does not exist, proceed silently. Domain-modeling work creates it only when a term or durable decision has been resolved.

## Use the glossary language

Use terms exactly as defined in `CONTEXT.md` in issue titles, tests, implementation plans, and code-facing documentation. Avoid synonyms that the glossary explicitly rejects. If a needed domain concept is missing, reconsider the term or add it through domain-modeling work.

## Respect decisions

Surface any conflict with an existing ADR instead of silently overriding it. A deliberate replacement should record which decision it supersedes and why.
