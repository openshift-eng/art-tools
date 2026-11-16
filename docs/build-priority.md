# Central image build priorities

`ocp-build-data/main/config/build-priority.yml` optionally overrides automatic
Konflux image build priorities across OCP and layered products. It is always
read from the canonical repository's `main`, regardless of a job's build-data
branch, ref, or fork. Each image build invocation loads one snapshot and logs
its SHA256 digest. It does not change already submitted PipelineRuns.

```yaml
enabled: false
rules:
  - product: ocp
    versions: ["4.19", "4.20", "4.21", "4.22", "5.0"]
    priority: 1
  - product: ocp
    priority: 3
default_priority: 2
```

With this example enabled, the listed development-cutoff OCP streams have
highest priority, layered products use priority 2, and remaining OCP streams
use priority 3. The stream list is maintained explicitly; this does not read a
release calendar or automatically promote future streams. All other products
use `default_priority`.

Rules are evaluated in order; the first match wins. Use canonical product IDs
(for example, `ocp` or `rhacm2`). `versions` is an optional list of major/minor
strings; `min_version: "4.18"` is also supported for a numeric lower bound.
Version selection uses the product version at the end of the group name.
A rule without version constraints matches every version of that product.
Priorities must be integers from 1 (highest) to 10 (lowest).

When `enabled: false`, or when the file is absent, existing behavior is retained:
Doozer uses image/group overrides and builder/lifecycle/dependency defaults;
`artcd build-layered-products` keeps its current explicit priority 3. An explicit
numeric Doozer `--build-priority` overrides the central policy and does not fetch
it. A missing `enabled` field defaults to false. Invalid configuration and
fetch failures stop submission instead of silently using a different policy.

Deploy the tooling before enabling the policy. Set `enabled: true` for an
incident and restore `false` afterward. Operators can reorder rules or place a
product-specific exception first when exploitability calls for a different
order. Changes take effect on subsequent invocations. Build dependencies are
still submitted in their existing order. Bundle/FBC release priorities and
Brew builds are outside this image-build policy.

Kueue admission still depends on the deployed priority classes, queue/cohort
configuration, quotas, and available hardware. This policy does not impose a
barrier requiring an entire product to finish before another starts.
