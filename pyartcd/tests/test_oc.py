import hashlib
import stat
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from pyartcd import oc


class TestExtractReleaseClientTools(TestCase):
    def test_retries_transport_error_with_clean_stage_and_verifies_output(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "clients"
            destination.mkdir()
            destination.chmod(0o755)
            stages = []

            def run_extract(cmd):
                stage = Path(next(arg.removeprefix("--to=") for arg in cmd if arg.startswith("--to=")))
                stages.append(stage)
                if len(stages) == 1:
                    (stage / "partial.tar.gz").write_bytes(b"partial")
                    return 1, "", "read: connection reset by peer"

                content = b"complete archive"
                (stage / "client.tar.gz").write_bytes(content)
                digest = hashlib.sha256(content).hexdigest()
                (stage / "sha256sum.txt").write_text(f"{digest}  client.tar.gz\n")
                return 0, "", ""

            with (
                patch.object(oc.exectools, "cmd_gather", side_effect=run_extract) as gather,
                patch.object(oc.time, "sleep") as sleep,
                patch.object(oc.random, "uniform", return_value=0),
            ):
                oc.extract_release_client_tools(
                    "quay.io/example/release:1", f"--to={destination}", "s390x", registry_config="/auth.json"
                )

            self.assertEqual(gather.call_count, 2)
            self.assertEqual(sleep.call_args.args, (30,))
            self.assertNotEqual(stages[0], stages[1])
            self.assertFalse(stages[0].exists())
            self.assertEqual((destination / "client.tar.gz").read_bytes(), b"complete archive")
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o755)
            self.assertFalse((destination / "partial.tar.gz").exists())
            self.assertIn("--max-per-registry=1", gather.call_args.args[0])
            self.assertIn("--filter-by-os=s390x", gather.call_args.args[0])
            self.assertIn("--registry-config=/auth.json", gather.call_args.args[0])

    def test_does_not_retry_authentication_error(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "clients"
            destination.mkdir()
            with (
                patch.object(oc.exectools, "cmd_gather", return_value=(1, "", "unauthorized")) as gather,
                patch.object(oc.time, "sleep") as sleep,
            ):
                with self.assertRaisesRegex(RuntimeError, "unauthorized"):
                    oc.extract_release_client_tools("quay.io/example/release:1", f"--to={destination}")
            gather.assert_called_once()
            sleep.assert_not_called()
            self.assertEqual(list(destination.iterdir()), [])

    def test_rejects_bad_checksum_without_publishing(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "clients"
            destination.mkdir()

            def run_extract(cmd):
                stage = Path(next(arg.removeprefix("--to=") for arg in cmd if arg.startswith("--to=")))
                (stage / "client.tar.gz").write_bytes(b"bad")
                (stage / "sha256sum.txt").write_text(f"{'0' * 64}  client.tar.gz\n")
                return 0, "", ""

            with (
                patch.object(oc.exectools, "cmd_gather", side_effect=run_extract) as gather,
                patch.object(oc.time, "sleep") as sleep,
            ):
                with self.assertRaisesRegex(RuntimeError, "checksum verification"):
                    oc.extract_release_client_tools("quay.io/example/release:1", f"--to={destination}")
            gather.assert_called_once()
            sleep.assert_not_called()
            self.assertEqual(list(destination.iterdir()), [])

    def test_stops_after_five_transport_failures(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "clients"
            destination.mkdir()
            with (
                patch.object(oc.exectools, "cmd_gather", return_value=(1, "", "read: connection timed out")) as gather,
                patch.object(oc.time, "sleep") as sleep,
                patch.object(oc.random, "uniform", return_value=0),
            ):
                with self.assertRaisesRegex(RuntimeError, "after 5 attempt"):
                    oc.extract_release_client_tools("quay.io/example/release:1", f"--to={destination}")
            self.assertEqual(gather.call_count, 5)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [30, 60, 120, 180])
            self.assertEqual(list(destination.iterdir()), [])
