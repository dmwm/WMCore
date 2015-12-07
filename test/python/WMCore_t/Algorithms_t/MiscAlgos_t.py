#!/usr/bin/env python
"""
_MiscAlgos_t_

Test class for Misc Algorithms
"""

import unittest

from WMCore.Algorithms import MiscAlgos

class MiscAlgosTest(unittest.TestCase):
    """
    _MiscAlgosTest_

    """

    def setUp(self):
        """
        _setUp_

        """

        return

    def tearDown(self):
        """
        _tearDown_

        """
        return

    def testA_SortListByKey(self):
        """
        _SortListByKey_

        Sort lists by key
        """

        d1 = [{'a': 1, 'b': 2, 'Title': 'First'}, {'a': 2, 'b': 1, 'Title': 'Second'}]

        result = MiscAlgos.sortListByKey(d1, 'a')
        self.assertEqual(result[1][0]['Title'], 'First')
        result = MiscAlgos.sortListByKey(d1, 'b')
        self.assertEqual(result[1][0]['Title'], 'Second')

        # Make sure it handles an empty set
        # This should print an error to logging, but skip the set in question
        d2 = [{'a': set(), 'Title': 'First'}, {'a': set([1]), 'Title': 'Second'}]
        result = MiscAlgos.sortListByKey(d2, 'a')
        self.assertEqual(result, {1: [{'a': set([1]), 'Title': 'Second'}]})

        return

if __name__ == "__main__":
    unittest.main()
