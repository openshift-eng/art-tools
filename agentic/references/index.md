# References

This section holds reference documentation for external systems and APIs that art-tools integrates with.

## External Systems

Art-tools interacts with several Red Hat internal systems. Reference docs for these can be added as needed:

- **Brew / Koji** -- Red Hat's build system. API documentation and common query patterns.
- **Errata Tool** -- Advisory management system. API endpoints and advisory lifecycle.
- **Bugzilla** -- Bug tracking. Query patterns and bug state transitions relevant to releases.
- **Jira** -- Issue tracking for ART team workflows and release planning.
- **Distgit (Dist-Git)** -- RPM and container source repositories managed via `rhpkg`.
- **UMB (Unified Message Bus)** -- Messaging system for build and release event notifications.
- **Konflux** -- Next-generation build system. Build pipeline integration points.

## Adding References

Add a reference document here when:

- You need to document API patterns, endpoints, or query templates for an external system
- A new external system integration is added to art-tools
- Common "how to query X" patterns emerge that should be shared across the team

Keep reference docs factual and concise. For "why we integrate this way," use an [ADR](../decisions/index.md).
