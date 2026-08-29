# tangos

## Documentation

The documentation is mid-rebuild: it is moving from a Jekyll site of loose markdown
files to Sphinx on Read the Docs, in stages, across many pull requests.

**Read `DOCS_PLAN.md` before changing anything under `docs/`.** It is the source of
truth for both the plan and the progress, and its "Decisions already made" section
records several things that look like omissions or mistakes but are deliberate —
you will otherwise undo them.

Stages branch off, and merge into, the long-lived `docs-refactor-main` branch rather
than `master`. Tick your stage's boxes in `DOCS_PLAN.md` in the same commit that
does the work.
