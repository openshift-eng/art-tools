from io import StringIO
from unittest import TestCase

from pyartcd import record


class TestRebuildPipeline(TestCase):
    def test_parse_record_log(self):
        fake_file = StringIO(
            "type1|key1=value1|key2=value2|\ntype2|key3=value3|key4=value4|\ntype2|key5=value5|\ntype3\n",
        )
        actual = record.parse_record_log(fake_file)
        expected = {
            "type1": [
                {"key1": "value1", "key2": "value2"},
            ],
            "type2": [
                {"key3": "value3", "key4": "value4"},
                {"key5": "value5"},
            ],
            "type3": [{}],
        }
        self.assertEqual(actual, expected)
