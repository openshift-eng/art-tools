import os

import requests
from ruamel.yaml import YAML

from . import exceptions, global_session


def fail_validation(msg, parsed):
    if not parsed:
        raise exceptions.ValidationFailed(msg)
    if "mode" in parsed and parsed["mode"] == "wip":
        raise exceptions.ValidationFailedWIP(msg)
    raise exceptions.ValidationFailed(msg)


def is_disabled(parsed):
    return "mode" in parsed and parsed["mode"] == "disabled"


def load_group_config_for(file):
    group_yaml = os.path.join(get_ocp_build_data_dir(file), "group.yml")
    return YAML(typ="safe").load(open(group_yaml).read())


def get_ocp_build_data_dir(file):
    file_path = os.path.dirname(file)
    if os.path.exists(os.path.join(file_path, "group.yml")):
        # File like releases.yml is already co-resident with group.yml
        obd_dir = file_path
    else:
        # image and rpm metas
        obd_dir = os.path.join(file_path, "..")
    return os.path.normpath(obd_dir)


def get_artifact_type(file):
    if "releases.yml" in file:
        return "releases"

    if "streams.yml" in file:
        return "streams"

    if "images/" in file:
        return "image"

    if "rpms/" in file:
        return "rpm"

    if "shipment" in file:
        return "shipment"

    if any([x in file for x in ["erratatool.yml", "group.yml", "bug.yml", "bugzilla.yml"]]):
        return "ignore"

    return "???"


def get_valid_streams_for(file):
    streams_yaml = os.path.join(get_ocp_build_data_dir(file), "streams.yml")
    return set(YAML(typ="safe").load(open(streams_yaml).read()).keys())


def get_valid_member_references_for(file):
    images_dir = os.path.join(get_ocp_build_data_dir(file), "images")
    return set([os.path.splitext(img)[0] for img in os.listdir(images_dir)])


def resource_exists(url):
    if url.startswith("https://github.com/openshift/ose-ovn-kubernetes"):
        # This is a private repository, and only used for 3.11. This will not change.
        return True
    if global_session.request_session:
        return 200 <= global_session.request_session.head(url).status_code < 400
    else:
        return 200 <= requests.head(url).status_code < 400


def resource_is_reachable(url):
    try:
        requests.head(url)
        return True
    except requests.exceptions.ConnectionError:
        return False


def get_namespace(data, file):
    if "distgit" in data and "namespace" in data["distgit"]:
        return data["distgit"]["namespace"]
    artifact_type = get_artifact_type(file)
    return {"image": "containers", "rpm": "rpms"}.get(artifact_type, "???")


def get_repository_name(file):
    return os.path.basename(file).split(".")[0]


def get_distgit_branch(data, group_cfg):
    if "distgit" in data and "branch" in data["distgit"]:
        return replace_vars(data["distgit"]["branch"], group_cfg["vars"])

    return replace_vars(group_cfg["branch"], group_cfg["vars"])


def replace_vars(text, vars_map):
    return text.replace("{MAJOR}", str(vars_map["MAJOR"])).replace("{MINOR}", str(vars_map["MINOR"]))
