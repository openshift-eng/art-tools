import json

from artcommonlib import exectools, logutil
from artcommonlib.model import ListModel, Model
from artcommonlib.runtime import GroupRuntime

# Historically the only RHCOS container was 'machine-os-content'; see
# https://github.com/openshift/machine-config-operator/blob/master/docs/OSUpgrades.md
# But with OCP 4.12 this changed, see
# https://github.com/coreos/enhancements/blob/main/os/coreos-layering.md
default_primary_container = dict(name="machine-os-content", build_metadata_key="oscontainer", primary=True)

logger = logutil.get_logger(__name__)


class RhcosMissingContainerException(Exception):
    """
    Thrown when group.yml configuration expects an RHCOS container but it is
    not available as specified in the RHCOS metadata.
    """

    pass


def get_container_configs(runtime: GroupRuntime):
    """
    look up the group.yml configuration for RHCOS container(s) for this group, or create if missing.
    @return ListModel with Model entries like ^^ default_primary_container
    """
    return runtime.group_config.rhcos.payload_tags or ListModel([default_primary_container])


def get_container_names(runtime: GroupRuntime):
    """
    look up the payload tags of the group.yml-configured RHCOS container(s) for this group
    @return list of container names
    """
    return {tag.name for tag in get_container_configs(runtime)}


def get_primary_container_conf(runtime: GroupRuntime):
    """
    look up the group.yml-configured primary RHCOS container for this group.
    @return Model with entries for name and build_metadata_key
    """
    for tag in get_container_configs(runtime):
        if tag.primary:
            return tag
    raise Exception("Need to provide a group.yml rhcos.payload_tags entry with primary=true")


def get_primary_container_name(runtime: GroupRuntime):
    """
    convenience method to retrieve configured primary RHCOS container name
    @return primary container name (used in payload tag)
    """
    return get_primary_container_conf(runtime).name


def get_container_pullspec(build_meta: dict, container_conf: Model) -> str:
    """
    determine the container pullspec from the RHCOS build meta and config
    @return full container pullspec string (registry/repo@sha256:...)
    """
    key = container_conf.build_metadata_key
    if key not in build_meta:
        raise RhcosMissingContainerException(
            f"RHCOS build {build_meta['buildid']} has no '{key}' attribute in its metadata"
        )

    container = build_meta[key]

    if 'digest' in container:
        # "oscontainer": {
        #   "digest": "sha256:04b54950ce2...",
        #   "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        # },
        return container['image'] + "@" + container['digest']

    # "base-oscontainer": {
    #     "image": "registry.ci.openshift.org/rhcos/rhel-coreos@sha256:b8e1064cae637f..."
    # },
    return container['image']


def get_build_id_from_rhcos_pullspec(pullspec, layered_id: bool = True) -> str:
    """
    Extract the RHCOS build ID from an image pullspec.
    - Starting from 4.16, the version is extracted from a new label "org.opencontainers.image.version". Prefer this if present and fall back to the "version" label if not.
    - Starting with 4.19, we also support layered RHCOS images, which have a label "coreos.build.manifest-list-tag" that contains the build ID for the image. The base rhel layer buildID is preserved in the "org.opencontainers.image.version" label.

    :param pullspec: The image pullspec to extract the build ID from.
    :param layered_id: If True, will attempt to extract the build ID from the "coreos.build.manifest-list-tag" label first if available, otherwise will use the "org.opencontainers.image.version" label.

    :return: The extracted build ID as a string.

    :raises:
    - ChildProcessError if the `oc image info` command fails to fetch the build info.
    - Exception if the required labels are not found in the image info.
    """

    logger.info(f"Looking up BuildID from RHCOS pullspec: {pullspec}")

    image_info_str, _ = exectools.cmd_assert(f'oc image info -o json {pullspec}', retries=3)
    image_info = Model(json.loads(image_info_str))
    labels = image_info.config.config.Labels

    # for layered rhcos it has label coreos.build.manifest-list-tag=4.19-9.6-202505081313-node-image-extensions
    # brew build name looks like rhcos-x86_64-4.19.9.6.202505081313-0 we need build_id 4.19.9.6.202505081313-0
    manifest_tag_label = labels.get('coreos.build.manifest-list-tag')
    image_version_label = labels.get('org.opencontainers.image.version')
    if layered_id and manifest_tag_label:
        list_tag = manifest_tag_label.split('-')
        build_id = f"{list_tag[0]}.{list_tag[1]}.{list_tag[2]}-0"
    elif image_version_label:
        build_id = image_version_label
    else:
        build_id = labels.version

    if not build_id:
        raise Exception(f'Unable to determine build_id from: {pullspec}. Retrieved image info: {image_info_str}')

    return build_id
