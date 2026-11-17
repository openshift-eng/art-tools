# Archived Brew rebuild implementation

`rebuild.py` contains the archived `RebuildPipeline`, including assembly-specific
plashet generation and injection through a custom `rebuild.repo`. The module has
no CLI registration and is not included in the installed `pyartcd` package.

Custom plashet injection has no confirmed equivalent in the supported Konflux
workflow. This code is kept for reference.

The original mocked tests are retained for reference and can be run explicitly
from the repository root in the development environment:

```sh
uv run pytest pyartcd/hack/test_rebuild.py
```
