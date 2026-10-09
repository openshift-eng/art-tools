"""Create the private OAuth credentials required by the Pipelines UI.

Run once after creating the namespace and Route. The credentials stay in the
cluster and are deliberately absent from GitOps manifests.
"""

import base64
import json
import secrets
import subprocess

NAMESPACE = "art-pipelines-ui"
NAME = "art-pipelines-ui-oauth"
CLIENT = "art-pipelines-ui"


def oc(*args: str, data: dict | None = None) -> dict:
    result = subprocess.run(
        ["oc", *args],
        input=json.dumps(data) if data is not None else None,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"oc {' '.join(args)} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip().startswith("{") else {}


def main() -> None:
    route = oc("get", "route", CLIENT, "-n", NAMESPACE, "-o", "json")
    redirect = f"https://{route['spec']['host']}/oauth/callback"
    existing = subprocess.run(
        ["oc", "get", "secret", NAME, "-n", NAMESPACE, "-o", "json"],
        text=True,
        capture_output=True,
        check=False,
    )
    if existing.returncode == 0:
        values = {key: base64.b64decode(value).decode() for key, value in json.loads(existing.stdout)["data"].items()}
    elif "NotFound" in existing.stderr:
        values = {"client-secret": secrets.token_hex(16), "cookie-secret": secrets.token_hex(16)}
    else:
        raise RuntimeError(f"oc get secret failed: {existing.stderr.strip()}")

    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": NAME, "namespace": NAMESPACE},
        "type": "Opaque",
        "stringData": values,
    }
    client = {
        "apiVersion": "oauth.openshift.io/v1",
        "kind": "OAuthClient",
        "metadata": {"name": CLIENT},
        "secret": values["client-secret"],
        "redirectURIs": [redirect],
        "grantMethod": "prompt",
    }
    oc("apply", "--server-side", "-f", "-", data={"apiVersion": "v1", "kind": "List", "items": [secret, client]})
    print(f"OAuth credentials and client are ready for {redirect}")


if __name__ == "__main__":
    main()
