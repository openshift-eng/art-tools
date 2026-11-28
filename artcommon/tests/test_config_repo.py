import unittest

from artcommonlib.config.repo import PlashetRepo, RepoList
from pydantic import ValidationError


def _source(from_name="rhaos-4.19-rhel-9-candidate", embargoed_tags=("rhaos-4.19-rhel-9-embargoed",)):
    return {
        "type": "brew",
        "from_tags": [
            {
                "name": from_name,
                "product_version": "OSE-4.19-RHEL-9",
                "release_tag": "rhaos-4.19-rhel-9",
                "inherit": False,
            }
        ],
        "embargoed_tags": list(embargoed_tags),
    }


def _public_repo(name="rhel-9-server-ose-rpms", **plashet_overrides):
    plashet = {
        "slug": "el9",
        "assembly_aware": True,
        "embargo_aware": True,
        "include_embargoed": False,
        "include_previous_packages": ["openshift"],
        "include_packages": [],
        "exclude_packages": [],
        "arches": ["x86_64"],
        "source": _source(),
    }
    plashet.update(plashet_overrides)
    return {"name": name, "type": "plashet", "plashet": plashet}


def _embargoed_repo(name="rhel-9-server-ose-rpms-embargoed", sibling="rhel-9-server-ose-rpms", **plashet_overrides):
    plashet = {
        "slug": "el9-embargoed",
        "assembly_aware": True,
        "embargo_aware": True,
        "include_embargoed": True,
        "include_previous_packages": ["openshift"],
        "include_packages": [],
        "exclude_packages": [],
        "arches": ["x86_64"],
        "source": _source(),
        "embargoed_sibling": sibling,
        "symlink_when_unembargoed": True,
    }
    plashet.update(plashet_overrides)
    return {"name": name, "type": "plashet", "plashet": plashet}


class TestPlashetRepoValidator(unittest.TestCase):
    def test_symlink_requires_include_embargoed(self):
        with self.assertRaises(ValidationError) as ctx:
            PlashetRepo.model_validate(
                {
                    "include_embargoed": False,
                    "symlink_when_unembargoed": True,
                    "embargoed_sibling": "rhel-9-server-ose-rpms",
                    "source": _source(),
                }
            )
        self.assertIn("include_embargoed", str(ctx.exception))

    def test_symlink_requires_embargoed_sibling(self):
        with self.assertRaises(ValidationError) as ctx:
            PlashetRepo.model_validate(
                {
                    "include_embargoed": True,
                    "symlink_when_unembargoed": True,
                    "source": _source(),
                }
            )
        self.assertIn("embargoed_sibling", str(ctx.exception))

    def test_valid_embargoed_repo(self):
        repo = PlashetRepo.model_validate(
            {
                "include_embargoed": True,
                "symlink_when_unembargoed": True,
                "embargoed_sibling": "rhel-9-server-ose-rpms",
                "source": _source(),
            }
        )
        self.assertEqual(repo.embargoed_sibling, "rhel-9-server-ose-rpms")
        self.assertTrue(repo.symlink_when_unembargoed)

    def test_defaults(self):
        repo = PlashetRepo.model_validate({"source": _source()})
        self.assertIsNone(repo.embargoed_sibling)
        self.assertFalse(repo.symlink_when_unembargoed)


class TestRepoListValidator(unittest.TestCase):
    def test_valid_pair(self):
        repo_list = RepoList.model_validate([_public_repo(), _embargoed_repo()])
        self.assertEqual(len(repo_list.root), 2)

    def test_sibling_does_not_exist(self):
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([_embargoed_repo(sibling="nonexistent-repo")])
        msg = str(ctx.exception)
        self.assertIn("nonexistent-repo", msg)
        self.assertIn("no repo with that name exists", msg)

    def test_sibling_is_embargoed(self):
        # The sibling is itself an embargoed repo -> must be refused (core safety assertion).
        bad_sibling = _public_repo(name="rhel-9-server-ose-rpms", include_embargoed=True)
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([bad_sibling, _embargoed_repo()])
        msg = str(ctx.exception)
        self.assertIn("itself embargoed", msg)
        self.assertIn("rhel-9-server-ose-rpms", msg)

    def test_sibling_not_plashet(self):
        external = {
            "name": "rhel-9-server-ose-rpms",
            "type": "external",
            "conf": {"baseurl": {"default": "http://example.com"}},
        }
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([external, _embargoed_repo()])
        self.assertIn("not a plashet repo", str(ctx.exception))

    def test_mismatched_source(self):
        public = _public_repo(source=_source(from_name="rhaos-4.19-rhel-9-different-candidate"))
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([public, _embargoed_repo()])
        msg = str(ctx.exception)
        self.assertIn("field 'source' differs", msg)

    def test_mismatched_include_previous_packages(self):
        public = _public_repo(include_previous_packages=["openshift", "podman"])
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([public, _embargoed_repo()])
        self.assertIn("include_previous_packages", str(ctx.exception))

    def test_mismatched_exclude_packages(self):
        public = _public_repo(exclude_packages=["kernel"])
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([public, _embargoed_repo()])
        self.assertIn("exclude_packages", str(ctx.exception))

    def test_mismatched_arches(self):
        public = _public_repo(arches=["x86_64", "aarch64"])
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([public, _embargoed_repo()])
        self.assertIn("arches", str(ctx.exception))

    def test_same_slug_is_rejected(self):
        # The embargoed repo and its public sibling must use DIFFERENT slugs; a shared slug
        # collapses both flavors into one directory.
        public = _public_repo(slug="el9")
        embargoed = _embargoed_repo(slug="el9")
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([public, embargoed])
        msg = str(ctx.exception)
        self.assertIn("different slugs", msg)
        self.assertIn("el9", msg)

    def test_unrelated_repos_same_slug_is_rejected(self):
        # Two UNRELATED plashet repos (no sibling link) that resolve to the same output
        # path/slug must be rejected globally, before any build runs.
        first = _public_repo(name="rhel-9-server-ose-rpms", slug="el9")
        second = _public_repo(name="rhel-9-other-ose-rpms", slug="el9")
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([first, second])
        msg = str(ctx.exception)
        self.assertIn("same", msg)
        self.assertIn("el9", msg)

    def test_slug_defaults_to_name_collision_is_rejected(self):
        # Slug resolves to repo name when unset; two repos resolving to the same name collide.
        first = _public_repo(name="dup-repo", slug=None)
        second = _public_repo(name="dup-repo-2", slug=None)
        # Give the second the same resolved slug as the first via default-to-name.
        second["name"] = "dup-repo"
        with self.assertRaises(ValidationError) as ctx:
            RepoList.model_validate([first, second])
        self.assertIn("dup-repo", str(ctx.exception))

    def test_no_sibling_declared_is_fine(self):
        # Ordinary repos without embargoed_sibling are unaffected.
        repo_list = RepoList.model_validate([_public_repo()])
        self.assertEqual(len(repo_list.root), 1)


if __name__ == "__main__":
    unittest.main()
