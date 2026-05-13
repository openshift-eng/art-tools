---
concept: Model/Missing
type: Pattern
related:
  - runtime
  - metadata
  - ocp-build-data
  - assembly
---

## Definition

Model and Missing are the safe YAML configuration traversal pattern used throughout art-tools. `Model` is a dict subclass that wraps Python dictionaries loaded from YAML files, returning a `Missing` singleton instead of raising `KeyError` when accessing undefined keys. `Missing` (an instance of `MissingModel`) is falsy and returns itself for any further attribute access, enabling arbitrarily deep safe traversal of configuration trees without try/except blocks or explicit existence checks at each level.

## Purpose

Model/Missing exists to eliminate defensive programming boilerplate when accessing deeply nested YAML configuration. Since ocp-build-data YAML files have varying structures (some components define certain keys, others do not), every config access would otherwise require nested `if key in dict` or try/except chains. With Model, code can write `if config.some.deeply.nested.optional.key:` and it will safely evaluate to False if any level in the chain is undefined, without throwing exceptions. This pattern is used pervasively across doozer, elliott, and artcommon.

## Location in Code

- **Module:** `artcommon/artcommonlib/model.py` -- Contains all four classes:
  - `Model(dict)` -- Dict subclass. `__getattr__` returns `Missing` for undefined keys, or wraps nested dicts/lists in Model/ListModel on access. `primitive()` recursively converts back to raw Python dicts.
  - `MissingModel(dict)` -- Empty dict subclass that is the Missing sentinel. `__bool__` returns False. `__getattr__` and `__getitem__` return `self` (enabling chained access). `__setattr__`, `__setitem__`, `__delattr__`, `__delitem__` all raise `ModelException` to prevent accidental mutation.
  - `Missing` -- The singleton `MissingModel()` instance, imported throughout the codebase as `from artcommonlib.model import Missing`.
  - `ListModel(list)` -- List subclass that wraps list elements in Model/ListModel on access. `primitive()` recursively converts back to raw Python lists.
  - `ModelException` -- Exception class for invalid operations on models.
  - `to_model_or_val(v)` -- Helper that wraps dicts as Model, lists as ListModel, and returns scalars unchanged.

## Lifecycle

1. **Creation:** Model objects are created whenever YAML data is loaded from ocp-build-data:
   - `MetadataBase.__init__()`: `self.raw_config = Model(data_obj.data)` and `self.config = assembly_metadata_config(...)` (which returns a Model).
   - `Runtime`: `self.group_config = Model(group_config_dict)`.
   - `Assembly functions`: `assembly_config_struct()` returns Model for dict defaults, ListModel for list defaults.
2. **Traversal:** Throughout tool code, config values are accessed via attribute syntax:
   ```python
   # Safe -- never throws, even if any intermediate key is missing
   if self.config.distgit.branch is not Missing:
       branch = self.config.distgit.branch

   # Boolean check -- Missing is falsy
   if self.config.content.source:
       source = self.config.content.source

   # Deep access with chaining
   network_mode = self.config.konflux.get("network_mode")
   ```
3. **Comparison:** `Missing` is compared using `is not Missing` (identity check) or truthiness (`if config.key:`). Since Missing is a singleton, identity checks are reliable.
4. **Conversion back:** `model.primitive()` recursively converts Model/ListModel back to plain dict/list, used when data needs to be serialized, merged, or passed to APIs that expect plain dicts.
5. **Mutation:** Model supports `__setattr__` and `__setitem__` for modification. MissingModel (Missing) raises `ModelException` on any mutation attempt, preventing accidental writes to undefined branches.

### Behavior Summary

| Operation | Model (key exists) | Model (key missing) | Missing |
|---|---|---|---|
| `obj.key` | Returns value (wrapped) | Returns `Missing` | Returns `Missing` |
| `obj[key]` | Returns value (wrapped) | Returns `Missing` | Returns `Missing` |
| `bool(obj)` | True (non-empty dict) | True (non-empty dict) | **False** |
| `obj.key = val` | Sets value | Sets value | **Raises ModelException** |
| `str(obj)` | Dict repr | Dict repr | `"(MissingModel)"` |
| `obj.primitive()` | Returns raw dict | Returns raw dict | N/A (dict methods) |

### Common Patterns in Codebase

```python
# Pattern 1: Check before use
if self.config.content.source is not Missing:
    # use self.config.content.source

# Pattern 2: Truthiness check (works because Missing is falsy)
if self.config.distgit.branch:
    branch = self.config.distgit.branch

# Pattern 3: Fallback with or
mode = self.config.get('mode', 'enabled')

# Pattern 4: Iteration over ListModel
for entry in self.config.targets:
    process(entry)

# Pattern 5: Convert to dict for serialization
raw = self.config.primitive()
```

## Related Concepts

- [runtime](runtime.md) -- Runtime's `group_config` and `releases_config` are Model objects, and Missing is used extensively in config access throughout initialization.
- [metadata](metadata.md) -- Every Metadata's `config` and `raw_config` are Model objects. All component configuration access uses the Model/Missing pattern.
- [ocp-build-data](ocp-build-data.md) -- All YAML data loaded from ocp-build-data is wrapped in Model objects for safe traversal.
- [assembly](assembly.md) -- Assembly merging functions (`_merger`, `assembly_config_struct`, etc.) operate on and return Model objects. Missing is used to detect undefined assembly fields.
