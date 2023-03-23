"""
Unit tests for the WMCore/MicroService/DataStructs/NanoWorkflow module
"""
import unittest

from WMCore.MicroService.MSTransferor.RequestInfo import isNanoWorkflow, rsesIntersection


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

    def testrsesIntersection(self):
        """
        Test the rsesIntersection function
        """
        rses = []
        self.assertEqual(rses, rsesIntersection(rses))

        rses = [['aaa', 'bbb', 'ccc']]
        self.assertEqual(rses[0], rsesIntersection(rses))

        rses = [['aaa', 'bbb', 'ccc'], ['bbb', 'ccc'], ['ccc']]
        self.assertEqual(rses[2], rsesIntersection(rses))

        rses = [['aaa', 'bbb', 'ccc'], ['ddd']]
        self.assertEqual([], rsesIntersection(rses))


if __name__ == '__main__':
    unittest.main()
