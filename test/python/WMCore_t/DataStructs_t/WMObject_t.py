#!/usr/bin/env python
"""
_WMObject_t_

Testcase for the WMObject class

"""

import unittest
from WMCore.DataStructs.WMObject import WMObject
from unittest import TestCase

class WMObjectTest(unittest.TestCase):
    """
    _WMObjectTest_

    Testcase for the WMObject class

    """

    def setup(self):
        """
        Initial Setup for the Job Testcase

        P.S: For some weird reason, i couldnt instantiate a
        WMObject as an attribute of WMObject_t.
        So, I used it as a local variable on each method.
        Not the best programming technique I guess, but it works
        until i figure out what i am doing wrong.
        """
        #self.dummyWMObject = WMObject(self)
        pass
    def tearDown(self):
        """
        No tearDown method for this Testcase
        """
        pass

    def testMakeList(self):
        """
        Testcase for the makelist method of the WMObject Class
        """
        dummyWMObject = WMObject()
        #First case: Argument is already a list
        l = [1,2,3]
        assert dummyWMObject.makelist(l) == l, \
                'method makelist return value doesn\'t match ' \
                '- list argument test'
        #Second case: Argument is not a list
        a = 'test'
        assert dummyWMObject.makelist(a) == [a], \
                'method makelist return value doesn\'t match ' \
                '- common argument test'

    def testMakeSet(self):
        """
        Testcase for the makeset method of the WMObject Class
        """
        dummyWMObject = WMObject()
        #First case: Argument is already a Set
        s = set('1,2,3')
        assert dummyWMObject.makeset(s) == s, \
                'method makeset return value doesn\'t match ' \
                '- list argument test'
        #Second case: Argument is not a list
        b = [1,2,3]
        assert dummyWMObject.makeset(b) == set(b), \
            'method makeset return value doesn\'t match ' \
            '- common argument test'

if __name__ == '__main__':
    unittest.main()
