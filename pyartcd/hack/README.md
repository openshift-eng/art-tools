# Archived Brew rebuild implementation

`rebuild.py` preserves the former `RebuildPipeline`, including assembly-specific
plashet generation and injection through a custom `rebuild.repo`. Its Jenkins
job and `artcd rebuild` command are retired. The archived module has no CLI
registration and is not included in the installed `pyartcd` package.

Use `build/ocp4-konflux` for supported image builds and the assembly-generation
jobs for assembly definitions. Custom plashet injection has no confirmed
equivalent in that workflow; retaining this source does not imply feature parity
or that it is ready to run against current infrastructure.

The original mocked tests are retained for reference and can be run explicitly
from the repository root in the development environment:

```sh
uv run pytest pyartcd/hack/test_rebuild.py
```
