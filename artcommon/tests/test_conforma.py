import pytest
from artcommonlib.conforma import resolve_stage_policy
from artcommonlib.product_catalog import PRODUCT_CATALOG


@pytest.mark.parametrize("product", ["rhacm2", "rhacm5", "multicluster-engine"])
@pytest.mark.parametrize("assembly", ["stream", "test"])
@pytest.mark.parametrize("phase", [None, "unknown", "release", "pre-release"])
@pytest.mark.parametrize("build_type", ["image", "bundle", "fbc"])
def test_acm_delivery_alias_preserves_stage_policies(product, assembly, phase, build_type):
    expected = (
        "rhtap-releng-tenant/fbc-art-acm-stage"
        if build_type == "fbc"
        else "rhtap-releng-tenant/registry-art-acm-stage"
    )
    policy = resolve_stage_policy(product, assembly=assembly, lifecycle_phase=phase, build_type=build_type)
    assert policy == expected
    assert policy == resolve_stage_policy("rhacm2", assembly=assembly, lifecycle_phase=phase, build_type=build_type)


@pytest.mark.parametrize("build_type", ["image", "bundle"])
@pytest.mark.parametrize("assembly", ["stream", "4.23.0-ec.1", "preview", "test"])
@pytest.mark.parametrize("phase", [None, "unknown", "signing", "release", "eol", "pre-release", "PRE_RELEASE"])
def test_ocp_stage_policy(build_type, assembly, phase):
    pre_release = phase in ("pre-release", "PRE_RELEASE")
    if assembly == "test":
        expected = (
            "ocp-art-tenant/conforma-build-ec-stage-test" if pre_release else "ocp-art-tenant/conforma-build-stage-test"
        )
    else:
        expected = (
            "rhtap-releng-tenant/registry-ocp-art-ec-stage"
            if pre_release
            else "rhtap-releng-tenant/registry-ocp-art-stage"
        )
    assert resolve_stage_policy("ocp", lifecycle_phase=phase, assembly=assembly, build_type=build_type) == expected


@pytest.mark.parametrize("assembly", ["stream", "test", "preview"])
@pytest.mark.parametrize("phase", [None, "release", "pre-release"])
def test_ocp_fbc_always_uses_fbc_stage(assembly, phase):
    assert resolve_stage_policy("ocp", lifecycle_phase=phase, assembly=assembly, build_type="fbc") == (
        "rhtap-releng-tenant/fbc-ocp-art-stage"
    )


@pytest.mark.parametrize("config", PRODUCT_CATALOG, ids=lambda config: config.product_name)
@pytest.mark.parametrize("assembly", ["stream", "test"])
@pytest.mark.parametrize("phase", ["release", "pre-release"])
def test_layered_catalog_policies_and_aliases(config, assembly, phase):
    if config.product_name == "ocp":
        return
    policies = config.conforma_stage_policies
    for product in (config.product_name, *config.aliases):
        for build_type in ("image", "bundle", "fbc"):
            expected = (
                None if policies is None else (policies.fbc_policy if build_type == "fbc" else policies.image_policy)
            )
            assert (
                resolve_stage_policy(product, lifecycle_phase=phase, assembly=assembly, build_type=build_type)
                == expected
            )


@pytest.mark.parametrize("build_type", ["image", "bundle", "fbc"])
@pytest.mark.parametrize("generic", [None, "tenant/override"])
@pytest.mark.parametrize("fbc", [None, "tenant/fbc-override"])
def test_override_precedence(build_type, generic, fbc):
    expected = fbc if build_type == "fbc" and fbc else generic
    assert resolve_stage_policy("unknown", build_type=build_type, ec_policy=generic, fbc_ec_policy=fbc) == expected


def test_mapping_corrections():
    assert resolve_stage_policy("quay", assembly="test") == "rhtap-releng-tenant/registry-art-quay-stage"
    assert resolve_stage_policy("mirror_gui") == "rhtap-releng-tenant/registry-art-oc-mirror-mirror-gui-stage"
    assert resolve_stage_policy("oc-mirror") == "rhtap-releng-tenant/registry-standard"
    assert resolve_stage_policy("mirror_gui", build_type="fbc") is None


def test_unsupported_build_type():
    with pytest.raises(ValueError, match="Unsupported Conforma build type"):
        resolve_stage_policy("ocp", build_type="rpm")


@pytest.mark.parametrize("product", [None, "", "unknown"])
def test_missing_or_unknown_product_has_no_default(product):
    assert resolve_stage_policy(product) is None
    assert resolve_stage_policy(product, ec_policy="tenant/override") == "tenant/override"
