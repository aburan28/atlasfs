# AGENTS.md

Instructions for AI coding agents (Claude Code, etc.) working in this repository.

## Project

`atlasfs` — see `README.md` for the current project description.

## End-of-conversation workflow

Every conversation that results in a code or file change must end with the
change committed and pushed, and a pull request opened — not left as
uncommitted local edits or a dangling branch.

1. **Work on a feature branch**, not directly on `main`. If the session
   already specifies a branch, use it; otherwise create one with a short,
   descriptive name (e.g. `agent/fix-parser-crash`).
2. **Commit before ending the conversation.** Stage only the files relevant
   to the request, and write a clear, descriptive commit message explaining
   *why* the change was made.
3. **Push the branch** to `origin` (`git push -u origin <branch-name>`).
4. **Open a pull request** targeting `main` once the branch is pushed.
   - Check for a PR template (`.github/pull_request_template.md`,
     `.github/PULL_REQUEST_TEMPLATE.md`, or `PULL_REQUEST_TEMPLATE.md`) and
     follow its structure if one exists.
   - Summarize what changed and why; include a short test plan if
     applicable.
5. **Do not skip this for "small" changes.** Any change worth committing is
   worth putting behind a PR for review — this keeps `main` protected and
   gives a human a natural checkpoint to review the agent's work.

If a task is purely exploratory (answering a question, reading code) and
produces no file changes, none of the above applies — only commit and open a
PR when there is an actual change to hand off.

## General conventions

- Prefer small, focused commits and PRs over large, mixed-purpose ones.
- Never force-push over a branch unless explicitly asked.
- Never commit secrets, credentials, or `.env` files.
