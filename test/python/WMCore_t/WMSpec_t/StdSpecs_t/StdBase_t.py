"""
_StdBase_t_

Created on Jun 14, 2013

@author: dballest
"""

import unittest
from nose.plugins.attrib import attr
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
            print k, arguments[k]
        stdBaseInstance = StdBase()
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        return
    
    @attr('integration')
    def testStdBaseIncludeParentsValidation(self):
        """
        _testStdBaseValidation_

        Check that the test arguments pass basic validation,
        i.e. no exception should be raised.
        """
        arguments = StdBase.getTestArguments()
        stdBaseInstance = StdBase()
        
        arguments["IncludeParents"] = True
        arguments["InputDataset"] = "/Cosmics/Commissioning2015-v1/RAW"
        self.assertRaises(WMSpecFactoryException, stdBaseInstance.factoryWorkloadConstruction, "TestWorkload", arguments)
        
        arguments["IncludeParents"] = True
        self.assertRaises(WMSpecFactoryException, stdBaseInstance.factoryWorkloadConstruction, "TestWorkload", arguments)
        
        arguments["IncludeParents"] = True
        arguments["InputDataset"] = "/Cosmics/Commissioning2015-6Mar2015-v1/RECO"
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        
        arguments["IncludeParents"] = False
        arguments["InputDataset"] = "/Cosmics/Commissioning2015-6Mar2015-v1/RECO"
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        
        arguments["IncludeParents"] = False
        arguments["InputDataset"] = "/Cosmics/ABS/RAW" 
        arguments["DbsUrl"] = None
        stdBaseInstance.factoryWorkloadConstruction("TestWorkload", arguments)
        return 

if __name__ == "__main__":
    unittest.main()
