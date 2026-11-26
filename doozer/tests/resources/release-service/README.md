# Release-service regression fixtures

These fixtures run the actual upstream collect-data jq merge programs and
check-data-keys schema offline. Do not replace them with a mock merge.

Source: https://github.com/konflux-ci/release-service-utils (Apache-2.0).

- `merge-legacy.jq`: `_MERGE_DEEP_UNION_ARRAYS_PROGRAM` from
  `src/helpers/json_merge.py` at `7a116200fa6b4a07fba8280f1614437192226758`.
  Arrays are unioned as whole objects; this reproduces the reported failure.
- `merge-current.jq`: the same program at
  `7b9b14697050b3d49d5aeef77a241574c9d4e566`, merging components by name.
- `dataKeys.json`: the complete `schemas/dataKeys.json` at
  `7b9b14697050b3d49d5aeef77a241574c9d4e566`.

The test uses collect-data source precedence: Release, ReleasePlan, then RPA.
It validates the result with the full draft-07 schema, checks all publishing
repositories and settings, and folds mapping entries as apply-mapping does.
