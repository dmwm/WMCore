#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-


from __future__ import division, print_function

import unittest

from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker


class testJSONThunker(unittest.TestCase):
    """
    Direct tests of thunking standard python type
    """

    def setUp(self):
        self.thunker = JSONThunker()

    def roundTrip(self, data):
        encoded = self.thunker.thunk(data)
        decoded = self.thunker.unthunk(encoded)
        self.assertEqual(data, decoded)

    def testStr(self):
        self.roundTrip('hello')

    def testList(self):
        self.roundTrip([123, 456])

    def testDict(self):
        self.roundTrip({'abc': 123, 'def': 456})
        self.roundTrip({'abc': 123, 456: 'def'})

    def testSet(self):
        self.roundTrip(set([]))
        self.roundTrip(set([123, 'abc']))


if __name__ == "__main__":
    unittest.main()
