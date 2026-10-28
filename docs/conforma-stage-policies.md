# Conforma stage-policy resolution

`artcommonlib.conforma.resolve_stage_policy` selects the policy from
`ProductConfig.conforma_stage_policies` for image ITS, OCP bundle ITS, scheduled
verification and ad-hoc `build-conforma-verify`. Policy selection does not decide
whether verification is eligible or enabled.

- OCP images and bundles use `rhtap-releng-tenant/registry-ocp-art-stage`, or
  `rhtap-releng-tenant/registry-ocp-art-ec-stage` when `software_lifecycle.phase`
  is `pre-release`. Assembly type, including preview, does not determine preGA.
  Missing or unknown lifecycle phases retain the scheduler's normal-policy fallback.
- OCP `test` assemblies retain `ocp-art-tenant/conforma-build-stage-test` and
  `ocp-art-tenant/conforma-build-ec-stage-test`. These are transitional exceptions:
  test base images are not released, and the art-images base registry rule is
  excluded. Normal stage policies are unchanged.
- OCP FBC verification uses `rhtap-releng-tenant/fbc-ocp-art-stage`, including
  pre-release and test assemblies. Layered products use their catalog image/FBC
  policies; a missing FBC policy does not fall back to an image policy.
- FBC-specific explicit overrides precede generic explicit overrides, which
  precede defaults. Unknown products have no default policy. Image ITS keeps
  its no-policy guard; verification pipelines reject missing policies clearly.

The former build-time policy maps are compatibility views derived from the
catalog. Quay now has its existing stage policy in the test-assembly view too,
so otherwise-eligible Quay test image builds receive verification rather than
being skipped due to a missing map entry. No eligibility gates change.

The catalog's `mirror_gui` image policy now matches its pre-existing ITS mapping,
`rhtap-releng-tenant/registry-art-oc-mirror-mirror-gui-stage`, rather than the
generic `registry-standard`. Scheduled and ad-hoc verification therefore preserve
that product-specific ITS policy. `oc-mirror` keeps `registry-standard`, and
neither product acquires an FBC policy. Catalog aliases resolve consistently.

Skip flags, OKD handling, `for_release`, `konflux.its.enabled`, successful-build
requirements and the OCP-only bundle ITS guard remain unchanged. This does not
enable layered-product bundle ITS or add a direct FBC build-time EC call.
