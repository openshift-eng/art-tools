import unittest
from artcommon.artcommonlib.util import TreeAnalyzer


class TestTreeAnalyzer(unittest.TestCase):
    def setUp(self):
        self.tree = {
            'root': {
                'child1': {
                    'child1_1': {},
                    'child1_2': {
                        'child1_2_1': {}
                    }
                },
                'child2': {
                    'child2_1': {},
                    'child2_2': {}
                },
                'child3': {}
            }
        }
        self.analyzer = TreeAnalyzer(self.tree)
        self.empty_tree = {}
        self.empty_analyzer = TreeAnalyzer(self.empty_tree)

    def test_get_elements_by_row(self):
        # Test elements by row for the example tree
        expected_result = [
            ['root'],
            ['child1', 'child2', 'child3'],
            ['child1_1', 'child1_2', 'child2_1', 'child2_2'],
            ['child1_2_1']
        ]
        self.assertEqual(self.analyzer.get_elements_by_row(), expected_result)

        # Test elements by row for an empty tree
        self.assertEqual(self.empty_analyzer.get_elements_by_row(), [])

    def test_find_element(self):
        # Test finding the root element
        self.assertEqual(self.analyzer.find_element(self.tree, 'root'), ['root'])

        # Test finding an intermediate element
        self.assertEqual(self.analyzer.find_element(self.tree, 'child1_2'), ['root', 'child1', 'child1_2'])

        # Test finding a leaf element
        self.assertEqual(self.analyzer.find_element(self.tree, 'child1_2_1'), ['root', 'child1', 'child1_2', 'child1_2_1'])

        # Test finding a non-existent element
        self.assertIsNone(self.analyzer.find_element(self.tree, 'nonexistent'))

        # Test finding an element in an empty tree
        self.assertIsNone(self.empty_analyzer.find_element(self.empty_tree, 'any'))

    def test_get_parents_and_children(self):
        # Test parents and children of the root element
        parents, children = self.analyzer.get_parents_and_children('root')
        self.assertEqual(parents, [])
        self.assertEqual(children, ['child1', 'child1_1', 'child1_2', 'child1_2_1', 'child2', 'child2_1', 'child2_2', 'child3'])

        # Test parents and children of an intermediate element
        parents, children = self.analyzer.get_parents_and_children('child1')
        self.assertEqual(parents, ['root'])
        self.assertEqual(children, ['child1_1', 'child1_2', 'child1_2_1'])

        # Test parents and children of a leaf element
        parents, children = self.analyzer.get_parents_and_children('child2_1')
        self.assertEqual(parents, ['root', 'child2'])
        self.assertEqual(children, [])

        # Test parents and children of a non-existent element
        parents, children = self.analyzer.get_parents_and_children('nonexistent')
        self.assertIsNone(parents)
        self.assertIsNone(children)

        # Test parents and children in an empty tree
        parents, children = self.empty_analyzer.get_parents_and_children('any')
        self.assertIsNone(parents)
        self.assertIsNone(children)


if __name__ == '__main__':
    unittest.main()
