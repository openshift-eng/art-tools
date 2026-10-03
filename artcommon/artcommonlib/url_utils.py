"""URL helpers shared by ART tools."""

from urllib import parse


def rewrite_ocp_artifacts_url(url: str) -> str:
    """Use the generated route until the ocp-artifacts custom route is restored."""
    old_host = "ocp-artifacts.engineering.redhat.com"
    new_host = "ocp-artifacts-art--runtime-int.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com"
    parts = parse.urlsplit(url)
    if parts.scheme == "https" and parts.netloc == old_host:
        return parts._replace(netloc=new_host).geturl()
    return url
