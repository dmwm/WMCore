#! /usr/bin/env python
"""
Unit testing base class with our extensions
"""

from __future__ import (division, print_function)
from future.utils import viewitems

import copy
import unittest


class ExtendedUnitTestCase(unittest.TestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.
    """

    def assertContentsEqual(self, expected_obj, actual_obj, msg=None):
        """
        A nested object comparison without regard for the ordering of contents. It asserts that
        expected_obj and actual_obj contain the same elements and that their sub-elements are the same.
        However, all sequences are allowed to contain the same elements, but in different orders.
        """

        def traverse_dict(dictionary):
            for key, value in viewitems(dictionary):
                if isinstance(value, dict):
                    traverse_dict(value)
                elif isinstance(value, list):
                    traverse_list(value)
            return

        def traverse_list(theList):
            for value in theList:
                if isinstance(value, dict):
                    traverse_dict(value)
                elif isinstance(value, list):
                    traverse_list(value)
            theList.sort()
            return

        if not isinstance(expected_obj, type(actual_obj)):
            self.fail(msg="The two objects are different type and cannot be compared: %s and %s" % (
            type(expected_obj), type(actual_obj)))

        expected = copy.deepcopy(expected_obj)
        actual = copy.deepcopy(actual_obj)

        if isinstance(expected, dict):
            traverse_dict(expected)
            traverse_dict(actual)
        elif isinstance(expected, list):
            traverse_list(expected)
            traverse_list(actual)
        else:
            self.fail(msg="The two objects are different type (%s) and cannot be compared." % type(expected_obj))

        return self.assertEqual(expected, actual)
