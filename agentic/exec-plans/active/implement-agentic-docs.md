---
status: active
owner: "@art-team"
created: 2026-04-16
target: 2026-04-30
related_issues: []
related_prs: []
---

# Implement Agentic Documentation Framework

## Goal

Implement an agentic documentation framework for art-tools to provide AI-agent-friendly navigation, domain knowledge, and exec-plan review workflow.

## Success Criteria

- [ ] 38 new files created under `agentic/`
- [ ] AGENTS.md is under 150 lines and serves as the primary entry point
- [ ] CI validation added for documentation structure
- [ ] All concept docs in `agentic/domain/concepts/` populated
- [ ] All component docs in `agentic/design-docs/components/` populated
- [ ] Exec-plan templates and tracker in place
- [ ] ADR template and initial ADRs written
- [ ] Developer reference docs populated

## Context

AI agents (Claude Code, Copilot, etc.) are increasingly used for development in art-tools. The existing `CLAUDE.md` provides project context, but a structured documentation framework will:

- Give agents deterministic navigation paths to find relevant context
- Capture domain knowledge that is otherwise tribal or scattered across wikis
- Replace ad-hoc PR descriptions with reviewable exec-plans
- Document architectural decisions in a discoverable format

This work is foundational and does not depend on external system changes.

## Technical Approach

### Architecture Changes

No code changes. This is a documentation-only addition under `agentic/`.

### New Abstractions

- **Exec-plan workflow:** Structured documents that replace lengthy PR descriptions for complex changes. Teammates review the plan before or alongside code.
- **AGENTS.md:** A concise entry point (<150 lines) that directs agents to the right documentation area.

### Dependencies

None. Pure documentation.

## Implementation Phases

### Phase 1: Directory Structure and Exec-Plans

- [x] Create `agentic/` directory tree
- [ ] Create exec-plan template (`agentic/exec-plans/template.md`)
- [ ] Create tech-debt tracker (`agentic/exec-plans/tech-debt-tracker.md`)
- [ ] Create this exec-plan (`agentic/exec-plans/active/implement-agentic-docs.md`)

### Phase 2: Navigation and Entry Points

- [ ] Create `AGENTS.md` at repo root (<150 lines)
- [ ] Create index files for each documentation area
  - [ ] `agentic/design-docs/index.md`
  - [ ] `agentic/domain/index.md`
  - [ ] `agentic/decisions/index.md`
  - [ ] `agentic/references/index.md`
  - [ ] `agentic/generated/README.md`

### Phase 3: Domain Documentation

- [ ] Create glossary (`agentic/domain/glossary.md`)
- [ ] Create concept docs in `agentic/domain/concepts/` (10 docs)
- [ ] Create workflow docs in `agentic/domain/workflows/` (3 docs)

### Phase 4: Design and Component Documentation

- [ ] Create core beliefs (`agentic/design-docs/core-beliefs.md`)
- [ ] Create component docs in `agentic/design-docs/components/` (5 docs)

### Phase 5: ADRs

- [ ] Create ADR template (`agentic/decisions/adr-template.md`)
- [ ] Write ADR-0001: Monorepo Structure for Release Tools
- [ ] Write ADR-0002: Runtime Pattern for CLI Initialization
- [ ] Write ADR-0003: Dual Build System Support (Brew and Konflux)

### Phase 6: Developer Reference Documentation

- [ ] Create developer reference docs in `agentic/references/`

### Phase 7: CI Validation

- [ ] Add CI check for documentation structure integrity
- [ ] Validate all index links resolve to existing files

## Testing Strategy

### Unit Tests

Not applicable (documentation only).

### Integration Tests

Not applicable.

### End-to-End Tests

- Manual review: verify all links in index files resolve
- CI validation script checks file existence and structure

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-04-16 | Use `agentic/` as the top-level directory | Separates agent-oriented docs from existing project docs; avoids cluttering repo root |
| 2026-04-16 | Keep AGENTS.md under 150 lines | Agents work better with concise entry points that link deeper rather than monolithic files |
| 2026-04-16 | Use exec-plans instead of RFCs | Exec-plans are lighter weight and action-oriented, better suited to this team's workflow |

## Progress Notes

| Date | Status Update |
|------|---------------|
| 2026-04-16 | Started implementation. Directory structure created. Working on exec-plan templates, index files, and ADR template. |

## Completion Checklist

- [ ] All implementation phases completed
- [ ] All index file links verified
- [ ] AGENTS.md under 150 lines
- [ ] PR(s) reviewed and merged
- [ ] Success criteria verified
- [ ] Exec plan status updated to `completed`
