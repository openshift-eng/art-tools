import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from elliottlib.exceptions import ElliottFatalError
from elliottlib.olm_operator_build_validation import select_konflux_olm_operator_build_for_find_builds


class TestSelectKonfluxOlmOperatorBuild(unittest.IsolatedAsyncioTestCase):
    @patch(
        "elliottlib.olm_operator_build_validation.validate_konflux_operator_build_manifest_refs", new_callable=AsyncMock
    )
    async def test_skips_invalid_then_returns_next(self, mock_validate):
        bad = MagicMock(spec=KonfluxBuildRecord, nvr="op-1.0-1")
        good = MagicMock(spec=KonfluxBuildRecord, nvr="op-1.0-0")
        image_meta = MagicMock()
        image_meta.distgit_key = "my-operator"
        image_meta.config = {}
        image_meta.list_konflux_success_build_candidates = AsyncMock(return_value=[bad, good])

        mock_validate.side_effect = [(False, "bad refs"), (True, None)]

        runtime = MagicMock()

        chosen, skipped = await select_konflux_olm_operator_build_for_find_builds(runtime, image_meta)

        self.assertEqual(chosen.nvr, good.nvr)
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["nvr"], bad.nvr)
        self.assertIn("bad refs", skipped[0]["reason"])

    @patch(
        "elliottlib.olm_operator_build_validation.validate_konflux_operator_build_manifest_refs", new_callable=AsyncMock
    )
    async def test_pinned_raises_on_invalid(self, mock_validate):
        pinned = MagicMock(spec=KonfluxBuildRecord, nvr="op-pinned-1.0-1")
        image_meta = MagicMock()
        image_meta.distgit_key = "my-operator"
        image_meta.config = {"is": {"nvr": "op-pinned-1.0-1"}}
        image_meta.list_konflux_success_build_candidates = AsyncMock(return_value=[pinned])
        mock_validate.return_value = (False, "broken")

        runtime = MagicMock()

        with self.assertRaises(ElliottFatalError) as ctx:
            await select_konflux_olm_operator_build_for_find_builds(runtime, image_meta)
        self.assertIn("Pinned", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
