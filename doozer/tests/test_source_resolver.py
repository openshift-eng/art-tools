
from unittest import TestCase
from flexmock import flexmock
from artcommonlib import exectools
from artcommonlib.model import Model
from doozerlib.source_resolver import SourceResolver


class SourceResolverTestCase(TestCase):
    @staticmethod
    def create_source_resolver():
        return SourceResolver(
            sources_base_dir="/path/to/sources",
            cache_dir="/path/to/cache",
            group_config=Model()
        )

    def test_get_remote_branch_ref(self):
        sr = self.create_source_resolver()
        flexmock(exectools).should_receive("cmd_assert").once().and_return("spam", "")
        res = sr._get_remote_branch_ref("giturl", "branch")
        self.assertEqual(res, "spam")

        flexmock(exectools).should_receive("cmd_assert").once().and_return("", "")
        self.assertIsNone(sr._get_remote_branch_ref("giturl", "branch"))

        flexmock(exectools).should_receive("cmd_assert").once().and_raise(Exception("whatever"))
        self.assertIsNone(sr._get_remote_branch_ref("giturl", "branch"))

    def test_detect_remote_source_branch(self):
        sr = self.create_source_resolver()
        source_details = dict(
            url='some_git_repo',
            branch=dict(
                target='main_branch',
                fallback='fallback_branch',
                stage='stage_branch',
            ),
        )

        # got a hit on the first branch
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("main_branch", "spam"), sr.detect_remote_source_branch(source_details, stage=False))

        # got a hit on the fallback branch
        (flexmock(SourceResolver).
            should_receive("_get_remote_branch_ref").
            and_return(None).
            and_return("eggs"))
        self.assertEqual(("fallback_branch", "eggs"), sr.detect_remote_source_branch(source_details, stage=False))

        # no target or fallback branch
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").and_return(None)
        with self.assertRaises(IOError):
            sr.detect_remote_source_branch(source_details, stage=False)

        # request stage branch, get it
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("stage_branch", "spam"), sr.detect_remote_source_branch(source_details, stage=True))

        # request stage branch, not there
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return(None)
        with self.assertRaises(IOError):
            sr.detect_remote_source_branch(source_details, stage=True)
