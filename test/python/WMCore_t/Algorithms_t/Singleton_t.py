#! /usr/bin/env python
"""
Unit tests for Singleton pattern
"""

__revision__ = "$Id: Singleton_t.py,v 1.1 2009/11/18 16:26:11 ewv Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
from WMCore.Algorithms.Singleton import Singleton

class SingletonTest(unittest.TestCase):
    """
    _SingletonTest_
    """

    def testIDs(self):
        """
        Make sure there really are two objects with the same underlying object
        """
        s1 = Singleton()
        s2 = Singleton()

        self.assertEqual(s1.singletonId(), s2.singletonId())
        self.assertNotEqual(id(s1), id(s2))



if __name__ == "__main__":
    unittest.main()
