#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the WMWorkloadTools module
"""
from __future__ import print_function, division

import unittest

from Utils.PythonVersion import PY3
from WMCore.WMSpec.WMWorkloadTools import checkMemCore, checkEventStreams, checkTimePerEvent


class WMWorkloadToolsTest(unittest.TestCase):

    def setUp(self):
        """
        Setup unit tests for this module
        """
        pass

    def tearDown(self):
        """
        Cleanup after the unit tests
        """
        pass

    def testCheckMemCore(self):
        """
        Tests for the function 'checkMemCore'
        """
        # input that fails validation
        self.assertFalse(checkMemCore(-1))
        self.assertFalse(checkMemCore(0))
        self.assertFalse(checkMemCore(0.5, minValue=1))
        self.assertFalse(checkMemCore(0, minValue=1))
        # input that passes validation
        self.assertTrue(checkMemCore(1))
        self.assertTrue(checkMemCore(0, minValue=0))
        self.assertTrue(checkMemCore(0.5, minValue=0))
        self.assertTrue(checkMemCore(1.0, minValue=1))
        self.assertTrue(checkMemCore(1, minValue=1))
        self.assertTrue(checkMemCore({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        self.assertTrue(checkMemCore({"Task1": 1, "Task2": 2, "Task3": 3.5}, minValue=1))
        self.assertTrue(checkMemCore({"Task1": 1, "Task2": 2, "Task3": 3.5}, minValue=0))
        # tests with different behaviour depending on the python version
        if PY3:
            self.assertFalse(checkMemCore("-1"))
            self.assertFalse(checkMemCore([1]))
            self.assertFalse(checkMemCore((1, 2)))
        else:
            self.assertTrue(checkMemCore("-1"))
            self.assertTrue(checkMemCore([1]))
            self.assertTrue(checkMemCore((1, 2)))

    def testCheckEventStreams(self):
        """
        Tests for the function 'checkEventStreams'
        """
        # input that fails validation
        self.assertFalse(checkEventStreams(-1))
        # input that passes validation
        self.assertTrue(checkEventStreams(1))
        self.assertTrue(checkEventStreams(0))
        self.assertTrue(checkEventStreams(0.5))
        self.assertTrue(checkEventStreams(1.0))
        self.assertTrue(checkEventStreams(1))
        self.assertTrue(checkEventStreams({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        self.assertTrue(checkEventStreams({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        self.assertTrue(checkEventStreams({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        # tests with different behaviour depending on the python version
        if PY3:
            self.assertFalse(checkEventStreams("-1"))
            self.assertFalse(checkEventStreams([1]))
            self.assertFalse(checkEventStreams((1, 2)))
        else:
            self.assertTrue(checkEventStreams("-1"))
            self.assertTrue(checkEventStreams([1]))
            self.assertTrue(checkEventStreams((1, 2)))


    def testCheckTimePerEvent(self):
        """
        Tests for the function 'checkTimePerEvent'
        """
        # input that fails validation
        self.assertFalse(checkTimePerEvent(-1))
        # input that passes validation
        self.assertTrue(checkTimePerEvent(1))
        self.assertTrue(checkTimePerEvent(0))
        self.assertTrue(checkTimePerEvent(0.5))
        self.assertTrue(checkTimePerEvent(1.0))
        self.assertTrue(checkTimePerEvent(1))
        self.assertTrue(checkTimePerEvent({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        self.assertTrue(checkTimePerEvent({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        self.assertTrue(checkTimePerEvent({"Task1": 1, "Task2": 2, "Task3": 3.5}))
        # tests with different behaviour depending on the python version
        if PY3:
            self.assertFalse(checkTimePerEvent("-1"))
            self.assertFalse(checkTimePerEvent([1]))
            self.assertFalse(checkTimePerEvent((1, 2)))
        else:
            self.assertTrue(checkTimePerEvent("-1"))
            self.assertTrue(checkTimePerEvent([1]))
            self.assertTrue(checkTimePerEvent((1, 2)))


if __name__ == '__main__':
    unittest.main()
