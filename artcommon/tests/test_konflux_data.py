import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from artcommonlib import util


class TestKonfluxData(unittest.IsolatedAsyncioTestCase):
    @patch("artcommonlib.util.cmd_gather_async", new_callable=AsyncMock)
    async def test_get_konflux_data_uses_most_specific_registry_auth(self, mock_cmd):
        observed_auth = {}

        async def run_cosign(_cmd, env):
            config_path = Path(env["DOCKER_CONFIG"]) / "config.json"
            with config_path.open(encoding="utf-8") as config_file:
                observed_auth.update(json.load(config_file)["auths"])
            return 0, "attestation", ""

        mock_cmd.side_effect = run_cosign
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as auth_file:
            json.dump(
                {
                    "auths": {
                        "quay.io/openshift": {"auth": "wrong"},
                        "quay.io/redhat-user-workloads/ocp-art-tenant/art-rhcos-images": {"auth": "rhcos"},
                    }
                },
                auth_file,
            )
            auth_file.flush()

            result = await util.get_konflux_data(
                "quay.io/redhat-user-workloads/ocp-art-tenant/art-rhcos-images@sha256:digest",
                registry_auth_file=auth_file.name,
            )

        self.assertEqual(result, "attestation")
        self.assertEqual(observed_auth["quay.io"]["auth"], "rhcos")
        self.assertEqual(mock_cmd.call_args.kwargs["env"]["REGISTRY_AUTH_FILE"], auth_file.name)


if __name__ == "__main__":
    unittest.main()
