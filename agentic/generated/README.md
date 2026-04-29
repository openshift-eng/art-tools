# Generated Documentation

This directory contains auto-generated documentation. **Do not manually edit files in this directory.** They will be overwritten by automated processes.

## Purpose

Generated docs provide derived or computed views of the codebase that are useful for navigation but expensive to maintain by hand. Examples of what could live here:

- **Dependency graph** -- Visual or textual representation of cross-package dependencies within the monorepo
- **API reference** -- Auto-generated CLI command documentation from docstrings and click decorators
- **Build metadata index** -- Summary of supported OCP versions and their build configurations

## Regeneration

Generated files should include a header comment indicating:
- The script or command that produced them
- The date of last generation
- Instructions to regenerate

If a generated file is out of date, re-run the generating command rather than editing the file directly.
