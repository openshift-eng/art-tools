import unittest

from validator import format


class TestFormat(unittest.TestCase):
    def test_invalid_yaml(self):
        invalid_yaml = """
        key: value
        - item#1
        - item#2
        """
        (parsed, err) = format.validate(invalid_yaml)
        self.assertIsNone(parsed)
        self.assertIn("did not find expected key", err)

    def test_valid_yaml(self):
        valid_yaml = """
        key: &my_list
          - 1
          - '2'
        obj:
          lst: *my_list
        """
        (parsed, err) = format.validate(valid_yaml)
        self.assertEqual(parsed, {'key': [1, '2'], 'obj': {'lst': [1, '2']}})
        self.assertIsNone(err)

    def test_duplicated_yaml(self):
        yml = """
        key: value
        key: value
        """
        (parsed, err) = format.validate(yml)
        self.assertIsNone(parsed)
        self.assertEqual(True, 'found duplicate key "key" with value "value" (original value: "value")' in err)
