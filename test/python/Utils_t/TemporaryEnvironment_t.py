#!/usr/bin/env python
"""
Unittests for TemporaryEnvironment function(s)
"""


import os
import unittest

from Utils.TemporaryEnvironment import tmpEnv


class TemporaryEnvironmentTest(unittest.TestCase):
    """
    unittest for IteratorTools functions
    """

    def testSetEnv(self):
        """
        Test the grouper function (returns chunk of an iterable)
        """

        with tmpEnv(NEW_ENV_VAR='blah'):
            self.assertTrue('NEW_ENV_VAR' in os.environ)
            self.assertEqual(os.environ['NEW_ENV_VAR'], 'blah')
        self.assertFalse('NEW_ENV_VAR' in os.environ)


if __name__ == '__main__':
    unittest.main()
