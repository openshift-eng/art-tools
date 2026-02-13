import re

from . import support


def validate(data, group_cfg):
    if not has_declared_github_repository(data):
        return (None, None)

    if not uses_ssh(data):
        repo_url = data["content"]["source"]["git"]["url"]
        return (None, f"Repository url {repo_url} does not use ssh")

    if not has_permitted_repo(data):
        repo_url = data["content"]["source"]["git"]["url"]
        return (None, f"Repository url {repo_url} is not in a known good upstream location")

    url = get_repository_url(data)

    if group_cfg and "public_upstreams" in group_cfg:
        url = translate_private_upstream_to_public(url, group_cfg)

    if not support.resource_exists(url):
        return (url, "GitHub repository {} doesn't exist".format(url))

    if not has_declared_branches(data):
        return (url, "No branches specified under content > source > git")

    (target, fallback) = get_branches(data, group_cfg)

    if not branch_exists(target, url) and not branch_exists(fallback, url):
        return (url, ("At least one of the following branches should exist: {} or {}".format(target, fallback)))

    branch = target if branch_exists(target, url) else fallback

    if has_declared_dockerfile(data):
        dockerfile = get_dockerfile(data)

        if not file_exists_on_repo(dockerfile, url, branch):
            return (url, ("dockerfile {} not found on branch {}".format(dockerfile, branch)))

    if has_declared_manifests(data):
        manifests = get_manifests_dir(data)

        if not file_exists_on_repo(manifests, url, branch):
            return (url, ("manifests {} not found on branch {}".format(manifests, branch)))

    return (url, None)


def has_declared_github_repository(data):
    return (
        "content" in data
        and "source" in data["content"]
        and "git" in data["content"]["source"]
        and "url" in data["content"]["source"]["git"]
    )


def get_repository_url(data):
    try:
        url = data["content"]["source"]["git"]["web"]
    except KeyError:
        url = (
            data["content"]["source"]["git"]["url"]
            .replace("git@github.com:", "https://github.com/")
            .replace(".git", "")
        )
    return url


def translate_private_upstream_to_public(url, group_cfg):
    for upstream in group_cfg["public_upstreams"]:
        url = url.replace(upstream["private"], upstream["public"])
    return url


def has_declared_branches(data):
    return (
        "content" in data
        and "source" in data["content"]
        and "git" in data["content"]["source"]
        and "branch" in data["content"]["source"]["git"]
        and (
            "target" in data["content"]["source"]["git"]["branch"]
            or "fallback" in data["content"]["source"]["git"]["branch"]
        )
    )


def get_branches(data, group_cfg):
    branch = data["content"]["source"]["git"]["branch"]
    target = (
        branch.get("target")
        .replace("{MAJOR}", str(group_cfg["vars"]["MAJOR"]))
        .replace("{MINOR}", str(group_cfg["vars"]["MINOR"]))
    )
    fallback = branch.get("fallback")
    return (target, fallback)


def branch_exists(branch, url):
    return support.resource_exists("{}/tree/{}".format(url, branch))


def has_declared_dockerfile(data):
    return "content" in data and "source" in data["content"] and "dockerfile" in data["content"]["source"]


def get_dockerfile(data):
    path = "{}/".format(get_custom_path(data)) if has_custom_path(data) else ""
    return "{}{}".format(path, data["content"]["source"]["dockerfile"])


def has_custom_path(data):
    return "content" in data and "source" in data["content"] and "path" in data["content"]["source"]


def get_custom_path(data):
    return data["content"]["source"]["path"]


def file_exists_on_repo(dockerfile, url, branch):
    dockerfile_url = "{}/blob/{}/{}".format(url, branch, dockerfile)
    return support.resource_exists(dockerfile_url)


def has_declared_manifests(data):
    return "update-csv" in data and "manifests-dir" in data["update-csv"]


def get_manifests_dir(data):
    path = "{}/".format(get_custom_path(data)) if has_custom_path(data) else ""
    return "{}{}".format(path, data["update-csv"]["manifests-dir"])


def uses_ssh(data):
    return data["content"]["source"]["git"]["url"].startswith("git@")


def has_permitted_repo(data):
    permitted = [
        "redhat-cne/cloud-event-proxy",  # max version 4.12
        "operator-framework/operator-lifecycle-manager",  # max version 4.7 3.11
        "operator-framework/operator-marketplace",  # max version 4.19
        "openshift/jenkins",  # max version 3.11
        "openshift/kubernetes-metrics-server",  # max version 3.11
        "openshift/kuryr-kubernetes",  # max version 3.11
        "openshift/oauth-proxy",  # max version 3.11
        "openshift/external-storage",  # max version 3.11
        "openshift/origin-web-console-server",  # max version 3.11
        "openshift/ose-ovn-kubernetes",  # max version 3.11
        "openshift/openshift-ansible",  # max version 3.11
        "openshift/origin-aggregated-logging",  # max version 3.11
        "openshift/kube-rbac-proxy",  # max version 3.11
        "openshift/grafana",  # max version 3.11
        "openshift/image-registry",  # max version 3.11
        "openshift/prometheus",  # max version 3.11
        "openshift/configmap-reload",  # max version 3.11
        "openshift/prometheus-operator",  # max version 3.11
        "openshift/descheduler",  # max version 3.11
        "openshift/kubernetes-autoscaler",  # max version 3.11
        "openshift/console",  # max version 3.11
        "openshift/cloud-provider-aws",  # max version 3.11
        "openshift/cluster-monitoring-operator",  # max version 3.11
        "openshift/service-catalog",  # max version 3.11
        "openshift/node_exporter",  # max version 3.11
        "openshift/kubernetes",  # config tweak
        "openshift/prometheus-alertmanager",  # max version 3.11
        "openshift/cluster-capacity",  # max version 3.11
        "openshift-eng/ocp-build-data",  # max version 4.19
        "openshift/node-problem-detector",  # max version 3.11
        "openshift/kube-state-metrics",  # max version 3.11
        "openshift/c2s-install",  # max version 3.11
        "org/repo",  # For testing
    ]
    regex = re.compile(r"^git@(?P<host>[^:]+):(?P<org>[^/]+)/(?P<repo>.*?)(\.git)?$")
    try:
        url = data["content"]["source"]["git"]["url"]
    except AttributeError:
        return False
    m = regex.match(url)
    if m is None:
        return False
    repo = m.groupdict()
    if repo["host"] != "github.com":
        return False
    if repo["org"] == "openshift-priv":
        return True
    if f"{repo['org']}/{repo['repo']}" in permitted:
        return True
    return False
