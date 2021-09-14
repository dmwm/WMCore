"""
_StdBase_t_

Created on Jun 14, 2013

@author: dballest
"""
from __future__ import print_function

import json
import unittest

from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


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
        for k in sorted(arguments.keys()):
            print(k, arguments[k])
        stdBaseInstance = StdBase()
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        return

    def testCalcEvtsPerJobLumi(self):
        """
        _testCalcEvtsPerJobLumi_

        Check that EventsPerJob and EventsPerLumi are properly calculated
        for EventBased job splitting.
        """
        self.assertEqual((123, 123), StdBase.calcEvtsPerJobLumi(123, 345, 1))
        self.assertEqual((123, 123), StdBase.calcEvtsPerJobLumi(123, None, 1))

        self.assertEqual((28800, 100), StdBase.calcEvtsPerJobLumi(None, 100, 1))
        self.assertEqual((600, 100), StdBase.calcEvtsPerJobLumi(None, 100, 50.5))
        self.assertEqual((570, 570), StdBase.calcEvtsPerJobLumi(None, 1000, 50.5))

        self.assertEqual((23040, 23040), StdBase.calcEvtsPerJobLumi(None, None, 1.25))
        self.assertEqual((229, 229), StdBase.calcEvtsPerJobLumi(None, None, 125.5))

        self.assertEqual((23528, 11764), StdBase.calcEvtsPerJobLumi(24000, 11764, 10.157120496967591))
        self.assertEqual((2835, 2835), StdBase.calcEvtsPerJobLumi(None, 11764, 10.157120496967591))

        self.assertEqual((10, 10), StdBase.calcEvtsPerJobLumi(123, 345, 1, requestedEvents=10))
        self.assertEqual((690, 345), StdBase.calcEvtsPerJobLumi(750, 345, 1, requestedEvents=700))
        self.assertEqual((15000, 100), StdBase.calcEvtsPerJobLumi(None, 100, 1, requestedEvents=15000))
        self.assertEqual((15000, 15000), StdBase.calcEvtsPerJobLumi(None, None, 1, requestedEvents=15000))

    def testValidateGPUSettings(self):
        """
        Test the 'validateGPUSettings' StdBase method.
        """
        with self.assertRaises(WMSpecFactoryException):
            StdBase.validateGPUSettings({"RequiresGPU": "optional"})
        with self.assertRaises(WMSpecFactoryException):
            StdBase.validateGPUSettings({"RequiresGPU": "required"})
        with self.assertRaises(WMSpecFactoryException):
            StdBase.validateGPUSettings({"RequiresGPU": "optional", "GPUParams": json.dumps("")})
        with self.assertRaises(WMSpecFactoryException):
            StdBase.validateGPUSettings({"RequiresGPU": "required", "GPUParams": json.dumps(None)})

        # now input that passes the validation
        self.assertTrue(StdBase.validateGPUSettings({"RequiresGPU": "forbidden"}))
        self.assertTrue(StdBase.validateGPUSettings({"RequiresGPU": "optional",
                                                     "GPUParams": json.dumps("blah")}))
        self.assertTrue(StdBase.validateGPUSettings({"RequiresGPU": "required",
                                                     "GPUParams": json.dumps({"Key1": "value1"})}))


if __name__ == "__main__":
    unittest.main()
