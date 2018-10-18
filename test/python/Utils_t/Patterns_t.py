"""
Patterns_t module provide unit tests for Patterns module
Unittests for Utilities functions
"""

from __future__ import division, print_function

import time
import unittest
from future.utils import with_metaclass
from Utils.Patterns import Singleton


class Test(with_metaclass(Singleton, object)):
    "Example of Singleton class"
    def __init__(self):
        self.time = time.time()

class PatternsTests(unittest.TestCase):
    """
    unittest for Patterns functions
    """

    def testSingleton(self):
        "Test singleton class"
        obj1 = Test()
        time.sleep(1)
        obj2 = Test()
        self.assertEqual(obj1.time, obj2.time)
        self.assertEqual(id(obj1), id(obj2))

if __name__ == '__main__':
    unittest.main()
