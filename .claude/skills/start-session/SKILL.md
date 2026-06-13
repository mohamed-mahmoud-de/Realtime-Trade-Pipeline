---
name: start-session
description: Start a new session. Reads project state from git and files, draws the current ASCII architecture, and proposes what to work on next. Use at the beginning of every session.
---

Read CLAUDE.md for project context and teaching rules.
Then explore the current state of the codebase:

1. Run: git log --oneline -10
2. List the folder structure
3. Read any files changed in the last few commits

Then do three things:

1. Draw the CURRENT state of the architecture in ASCII.
   Only include components that are actually built so far.
   Mark unbuilt components as (not yet).

2. Tell me where we are in the session map from CLAUDE.md.

3. Give me 2-3 options for what to work on today.

Wait for me to choose before doing anything.
Do NOT write any code. Do NOT make any changes.
