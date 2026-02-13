import datetime
import logging
import sys
import traceback
import unittest
from multiprocessing import RLock

from doozerlib.dblib import DB, Record


class FakeMetaData(object):
    def __init__(self):
        self.name = "test"
        self.namespace = "test_namespace"
        self.qualified_key = "test_qualified_key"
        self.qualified_name = "test_qualified_name"


class FakeRuntime(object):
    """This is a fake runtime class to inject into dblib running tests."""

    mutex = RLock()

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Create a "uuid" which will be used in FROM fields during updates
        self.uuid = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")

        self.user = ""

        self.group_config = dict()
        self.group_config["name"] = "test"

    @staticmethod
    def timestamp():
        return datetime.datetime.utcnow().isoformat()


class DBLibTest(unittest.TestCase):
    def setUp(self):
        self.setup_failed = False
        try:
            self.fake_runtime = FakeRuntime()
            self.db = DB(runtime=self.fake_runtime, environment="test")
        except Exception:
            traceback.print_exc()
            self.setup_failed = True

    def test_select_withoutenv(self):
        if not self.setup_failed:
            self.assertRaises(RuntimeError, self.db.select, "select * from test", 10)

    def test_record(self):
        if not self.setup_failed:
            try:
                with self.db.record(operation="build", metadata=None):
                    Record.set("name", "test")
                    Record.set("position", "record")

                with self.db.record(operation="build", metadata=None):
                    Record.set("name", "test2")
                    Record.set("position", "record2")
                    Record.set("position2", "r_record2")

            except Exception:
                self.fail(msg="Failed to record.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def test_record_with_metadata(self):
        if not self.setup_failed:
            try:
                with self.db.record(operation="build", metadata=FakeMetaData()):
                    Record.set("name", "test")
                    Record.set("position", "record")
                    Record.set("country", "USA")
                    Record.set("population", 45435432523)
            except Exception:
                self.fail(msg="Failed to create record with extras.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def test_record_with_empty_value(self):
        if not self.setup_failed:
            try:
                with self.db.record(operation='build', metadata=None):
                    Record.set("name", "test")
                    Record.set("position", None)
                    Record.set("country", "")
                    Record.set("population", 0)
            except Exception:
                self.fail(msg="Failed to create record with missing attribute value.")
        else:
            self.skipTest(reason="DB setup failed for running test.")

    def tearDown(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    unittest.main()
