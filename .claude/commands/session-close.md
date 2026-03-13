---
type: skill
title: "/session-close"
summary: "Capture session summary with decisions, artifacts, insights, and next steps"
status: sprout
ring: mid
tags: [skill, session, continuity, phase/v1]
created: "2026-03-03"
modified: "2026-03-04"
skill_path: "~/.claude/commands/session-close.md"
triggers:
  - "User types /session-close"
  - "User is ending a work session"
parameters: []
relationships:
  - "[[briefing]]"
  - "[[vault]]"
mastery: intermediate
times_used: 0
last_used: ""
proficiency_notes: ""
---

## Purpose

Capture a summary of the current session including what was accomplished, decisions made, artifacts produced, problems encountered, key insights, and next steps. Saves to `journal/sessions/`.

## Triggers

- User types `/session-close`
- End of a work session

## Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| (none) | — | Auto-generates from session context | — |

## Usage

```
/session-close
```

## Related Skills

- [[briefing]] — reads session notes at start of next session
- [[vault]] — general note creation

## Notes

- Session notes named: `YYYY-MM-DD-HH-summary.md`
- Token budget: MAX 500 tokens written to note
- Provides continuity between Claude Code sessions
