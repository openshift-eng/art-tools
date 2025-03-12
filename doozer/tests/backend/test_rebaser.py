from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from doozerlib.backend.rebaser import KonfluxRebaser


class TestRebaser(IsolatedAsyncioTestCase):
    async def test_find_previous_versions(self):
        metadata = MagicMock()
        metadata.get_component_name.return_value = "component_name"
        runtime = MagicMock()
        mock_builds = [
            MagicMock(version="v4.19.0", release="202503061643.p0.g8c8af5f.assembly.stream.el9"),
            MagicMock(version="v4.19.0", release="202503051607.p0.g8c8af5f.assembly.stream.el9"),
            MagicMock(version="v4.19.0", release="202503031619.p0.g8c8af5f.assembly.stream.el9"),
            MagicMock(version="v4.19.0", release="202502272307.p0.g8d02152.assembly.stream.el9"),
        ]

        async def search_builds_by_fields(**_):
            for build in mock_builds:
                yield build
        runtime.konflux_db.search_builds_by_fields = search_builds_by_fields

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=MagicMock(),
            source_resolver=MagicMock(),
            repo_type=MagicMock(),
        )

        results = await rebaser._find_previous_versions(metadata)
        self.assertEqual(results, {
            "4.19.0-202503061643", "4.19.0-202503051607", "4.19.0-202503031619", "4.19.0-202502272307"
        })
