"""
_StdBase_t_

Created on Jun 14, 2013

@author: dballest
"""

import unittest

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

class StdBaseTest(unittest.TestCase):
    """
    _StdBaseTest_

    A test class for StdBase, it includes tests for
    basic validation functionality.
    """

    def setUp(self):
        """
        _setUp

        Nothing to do
        """
        pass

    def tearDown(self):
        """
        _tearDown_

        Nothing to do
        """
        pass

    def testStdBaseValidation(self):
        """
        _testStdBaseValidation_

        Check that the test arguments pass basic validation,
        i.e. no exception should be raised.
        """
        arguments = StdBase.getTestArguments()
        print
        for k in sorted(arguments.keys()):
            print k, arguments[k]
        stdBaseInstance = StdBase()
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        return

if __name__ == "__main__":
    unittest.main()
