"""
Patterns_t module provide unit tests for Patterns module
Unittests for Utilities functions
"""
import time
import unittest
from Utils.Patterns import Singleton, getDomainName


class Test(object, metaclass=Singleton):
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


    def testGetDomainName(self):
        "Test the getDomainName function"
        # test with http protocol
        self.assertEqual(getDomainName("http://cmsweb.cern.ch/blah/two"), "cmsweb")
        # test with https protocol
        self.assertEqual(getDomainName("https://cmsweb.cern.ch/blah/two"), "cmsweb")
        self.assertEqual(getDomainName("https://cmsweb-testbed.cern.ch"), "cmsweb-testbed")
        self.assertEqual(getDomainName("https://cmsweb-preprod.cern.ch"), "cmsweb-preprod")
        self.assertEqual(getDomainName("https://cmsweb-test3.cern.ch"), "cmsweb-test3")
        # test with unexpected/wrong URL
        self.assertEqual(getDomainName("https://cmsweb.blah.ch"), "")


if __name__ == '__main__':
    unittest.main()
