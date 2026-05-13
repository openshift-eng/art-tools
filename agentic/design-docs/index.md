# Design Documentation

## Core

- [Core Beliefs](./core-beliefs.md) -- Guiding principles and architectural values for art-tools

## Components

- [artcommon](./components/artcommon.md) -- Shared library used by doozer, elliott, and pyartcd
- [doozer](./components/doozer.md) -- CLI tool for managing OCP builds (RPMs and container images)
- [elliott](./components/elliott.md) -- CLI tool for managing release advisories, errata, and bugs
- [pyartcd](./components/pyartcd.md) -- Automated release pipeline code
- [ocp-build-data-validator](./components/ocp-build-data-validator.md) -- Schema validator for ocp-build-data

## When to Add Here

Add a document to this section when:

- A new component is added to the monorepo
- Core architectural beliefs are updated or refined
- A component's design evolves enough to warrant documentation beyond inline comments
- You need to explain "why the code is shaped this way" rather than "what the code does"

For architectural decisions (choosing between alternatives), use an [ADR](../decisions/index.md) instead.
