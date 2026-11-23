# Central image build priorities

`ocp-build-data/main/config/build-priority.yml` optionally overrides automatic
Konflux image build priorities across OCP, OKD, and layered products. It is always
read from the canonical repository's `main`, regardless of a job's build-data
branch, ref, or fork. Each image build invocation loads one snapshot and logs
its SHA256 digest. It does not change already submitted PipelineRuns.

```yaml
enabled: false
rules:
  - product: okd
    priority: 8
  - product: ocp
    versions: ["4.19", "4.20", "4.21", "4.22", "5.0"]
    priority: 2
  - product: ocp
    priority: 4
default_priority: 3
```

With this example enabled, OKD uses priority 8, the listed development-cutoff
OCP streams use priority 2, layered products use priority 3, and remaining OCP
streams use priority 4. The stream list is maintained explicitly; this does not
read a release calendar or automatically promote future streams. All other
products use `default_priority`. OKD's priority can be changed centrally
without changing the per-stream group configuration.

OKD builds use `product: okd` from the OKD section of `group.yml`, merged into
the group configuration when Doozer runs with `--variant=okd`. When the central
policy is disabled, the group's `okd.konflux.build_priority` is the fallback
unless an image-specific priority is set. An explicit numeric
`--build-priority` takes precedence over the central policy.
The OKD Jenkins job passes `--build-priority=auto` to `artcd okd`. Direct
`artcd okd` invocations must request `auto` to use the central policy; their
default remains priority 10. Doozer image builds default to `auto`.

Golang builders bypass policy loading and resolution, retaining their existing
priority 2 default regardless of `enabled`. Release FBCs continue to request
priority 2 directly through release preparation. Neither category needs a rule
or depends on this policy being enabled. Explicit overrides keep their existing
behavior for these builds.

Rules are evaluated in order; the first match wins. Use canonical product IDs
(for example, `ocp` or `rhacm2`). `versions` is an optional list of major/minor
strings; `min_version: "4.18"` is also supported for a numeric lower bound.
Version selection uses the product version at the end of the group name.
A rule without version constraints matches every version of that product.
Policy priorities must be integers from 2 (highest allowed) to 10 (lowest).
Priority 1 is reserved and rejected in both rules and `default_priority`.

When `enabled: false`, or when the file is absent, Doozer resolves automatic
priorities from image/group overrides and builder/lifecycle/dependency defaults;
`artcd build-layered-products` uses priority 3. An explicit numeric Doozer
`--build-priority` bypasses the policy. A missing `enabled` field defaults to
false. Invalid configuration and fetch failures stop submission.

Deploy the tooling before enabling the policy. Set `enabled: true` for an
incident and restore `false` afterward. Operators can reorder rules or place a
product-specific exception first when exploitability calls for a different
order. Changes take effect on subsequent invocations. Build dependencies are
still submitted in their existing order. Bundle/FBC release priorities and
Brew builds are outside this image-build policy.

Kueue admission still depends on the deployed priority classes, queue/cohort
configuration, quotas, and available hardware. This policy does not impose a
barrier requiring an entire product to finish before another starts.
