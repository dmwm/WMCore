"""
Unit tests for the WMCore/MicroService/DataStructs/NanoWorkflow module
"""
import unittest

from WMCore.MicroService.MSTransferor.RequestInfo import isNanoWorkflow


class RequestInfoTest(unittest.TestCase):
    """
    Test the very basic functionality of the RequestInfo module
    """

    def setUp(self):
        """
        Defined some basic data structs to use in the unit tests
        :return: None
        """
        pass

    def tearDown(self):
        pass

    def testIsNanoWorkflow(self):
        """
        Test object instance type
        """
        testDict = {'RequestType': 'ReReco'}
        self.assertFalse(isNanoWorkflow(testDict))
        testDict = {'RequestType': 'ReReco', 'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAODSIM'}
        self.assertFalse(isNanoWorkflow(testDict))

        testDict = {'RequestType': 'TaskChain', 'Task1': {}, 'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAODSIM'}
        self.assertFalse(isNanoWorkflow(testDict))
        testDict = {'RequestType': 'TaskChain', 'Task1': {'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAOD'}}
        self.assertFalse(isNanoWorkflow(testDict))
        testDict = {'RequestType': 'TaskChain', 'Task1': {'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAODSIM'}}
        self.assertTrue(isNanoWorkflow(testDict))

        testDict = {'RequestType': 'StepChain', 'Step1': {}, 'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAODSIM'}
        self.assertFalse(isNanoWorkflow(testDict))
        testDict = {'RequestType': 'StepChain', 'Step1': {'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAOD'}}
        self.assertFalse(isNanoWorkflow(testDict))
        testDict = {'RequestType': 'StepChain', 'Step1': {'InputDataset': '/PrimDset/AcqEra-ProcStr/MINIAODSIM'}}
        self.assertTrue(isNanoWorkflow(testDict))


if __name__ == '__main__':
    unittest.main()
