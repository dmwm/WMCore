#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from __future__ import division, print_function
from pprint import pprint
import unittest
from copy import deepcopy
from WMCore.ReqMgr.DataStructs.Request import initialize_clone
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException

### Spec arguments definition with only key and its default value
MC_ARGS = MonteCarloWorkloadFactory.getWorkloadCreateArgs()
RERECO_ARGS = ReRecoWorkloadFactory.getWorkloadCreateArgs()
STEPCHAIN_ARGS = StepChainWorkloadFactory.getWorkloadCreateArgs()
TASKCHAIN_ARGS = TaskChainWorkloadFactory.getWorkloadCreateArgs()
# inner Task/Step definition
STEP_ARGS = StepChainWorkloadFactory.getChainCreateArgs()
TASK_ARGS = TaskChainWorkloadFactory.getChainCreateArgs()

### Some original request dicts (ones to be cloned from)
mcOriginalArgs = {'Memory': 1234, 'TimePerEvent': 1.2, 'RequestType': 'MonteCarlo', "LheInputFiles": True,
                  "ScramArch": ["slc6_amd64_gcc481"], "RequestName": "test_mc"}
resubOriginalArgs = {'Memory': 1234, 'TimePerEvent': 1.2, 'RequestType': 'Resubmission', "ProcessingVersion": 7,
                     "ScramArch": ["slc6_amd64_gcc481"], "RequestName": "test_resub"}
rerecoOriginalArgs = {'Memory': 234, 'SkimName1': 'skim_2017', 'SkimInput1': 'RECOoutput', 'Skim1ConfigCacheID': 'abcdef',
                      'TimePerEvent': 1.2, 'RequestType': 'ReReco', 'RequestName': 'test_rereco'}
stepChainOriginalArgs = {'Memory': 1234, 'TimePerEvent': 1.2, 'RequestType': 'StepChain',
                         "ScramArch": ["slc6_amd64_gcc481"], "RequestName": "test_stepchain",
                         "Step1": {"ConfigCacheID": "blah", "GlobalTag": "PHYS18", "BlockWhitelist": ["A", "B"]},
                         "Step2": {"AcquisitionEra": "ACQERA", "ProcessingVersion": 3, "LumisPerJob": 10},
                         "StepChain": 2, "ConfigCacheID": None}
taskChainOriginalArgs = {'PrepID': None, 'Campaign': "MainTask", 'RequestType': 'TaskChain',
                         "ScramArch": ["slc6_amd64_gcc481", "slc7_amd64_gcc630"], "RequestName": "test_taskchain",
                         "Task1": {"ConfigCacheID": "blah", "InputDataset": "/1/2/RAW", "BlockWhitelist": ["A", "B"],
                                   "LumiList": {"202205": [[1, 10], [20, 25]], "202209": [[1, 3], [5, 5], [6, 9]]}},
                         "Task2": {"AcquisitionEra": "ACQERA", "KeepOutput": False, "ProcessingVersion": 2,
                                   "LumisPerJob": 10},
                         "Task3": {"InputTask": "task111", "TaskName": "blah", "LumisPerJob": 10},
                         "TaskChain": 3, "DQMConfigCacheID": 'unidunite',
                         "ProcessingVersion": {"name1": 4, "name2": 5, "name3": 6}}


def updateDict(dictObj1, dictObj2):
    """
    Util for updating the dictObj1 with the context of dictObj2 and return the final dict
    """
    dictObj1.update(dictObj2)
    return dictObj1


class RequestTests(unittest.TestCase):
    """
    unittest for ReqMgr DataStructs Request functions.
    """

    def testInvalidKeys_initialize_clone(self):
        """
        Test some undefined/assignment user arguments in initialize_clone
        """
        # test with unknown key
        requestArgs = {'Multicore': 1, 'WRONG': 'Alan'}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        with self.assertRaises(WMSpecFactoryException):
            MonteCarloWorkloadFactory().factoryWorkloadConstruction(cloneArgs["RequestName"], cloneArgs)

        # test with assignment key
        requestArgs = {'Multicore': 1, 'TrustSitelists': False}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        with self.assertRaises(WMSpecFactoryException):
            MonteCarloWorkloadFactory().factoryWorkloadConstruction(cloneArgs["RequestName"], cloneArgs)

        # test mix of StepChain and TaskChain args (it passes the initial filter but raises factory exception)
        requestArgs = {'SubRequestType': "RelVal", "Step2": {"LumisPerJob": 5, "Campaign": "Step2Camp"}}
        originalArgs = deepcopy(taskChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        with self.assertRaises(WMSpecFactoryException):
            TaskChainWorkloadFactory().factoryWorkloadConstruction(cloneArgs["RequestName"], cloneArgs)

        # make sure unsupported keys are remove from both top and inner dicts
        requestArgs = {}
        originalArgs = deepcopy(taskChainOriginalArgs)
        originalArgs['BAD'] = 'KEY'
        originalArgs['Task1']['SiteWhitelist'] = ['This_key_should_not_be_here']
        originalArgs['Task1']['Alan'] = False
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        self.assertFalse('BAD' in cloneArgs)
        self.assertFalse('SiteWhitelist' in cloneArgs['Task1'])
        self.assertFalse('Alan' in cloneArgs['Task1'])

    def testMC_initialize_clone(self):
        """
        Test the basics of initialize_clone for MC requests
        """
        # test overwrite of original values
        requestArgs = {'Multicore': 1, 'ScramArch': 'slc6_amd64_gcc481', 'LheInputFiles': False}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))

        # test overwrite of values that are not in the original request
        requestArgs = {'DQMUploadProxy': 'test_file', 'RequestNumEvents': 1, 'DQMSequences': ['a', 'b']}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))

        # check whether invalid keys in the original request are dumped
        requestArgs = {'RequestNumEvents': 1, 'DQMSequences': []}
        originalArgs = deepcopy(mcOriginalArgs)
        originalArgs.update(badJob='whatever', WMCore={'YouFool': True})
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))

    def testReReco_initialize_clone(self):
        """
        Test the basics of initialize_clone for a ReReco request. 
        """
        # test overwrite of original values
        requestArgs = {'TimePerEvent': 21, 'Multicore': 1, 'SkimName1': 'new_skim_name'}
        originalArgs = deepcopy(rerecoOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, RERECO_ARGS)
        rerecoArgs = deepcopy(rerecoOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(rerecoArgs, requestArgs))

        # test overwrite of values that are not in the original request
        requestArgs = {'DQMUploadProxy': 'test_file', 'RequestNumEvents': 1, 'DQMSequences': ['a', 'b']}
        originalArgs = deepcopy(rerecoOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, RERECO_ARGS)
        rerecoArgs = deepcopy(rerecoOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(rerecoArgs, requestArgs))

        # check whether invalid keys in the original request are dumped
        requestArgs = {'RequestNumEvents': 1, 'DQMSequences': []}
        originalArgs = deepcopy(rerecoOriginalArgs)
        originalArgs.update(badJob='whatever', WMCore={'YouFool': True})
        cloneArgs = initialize_clone(requestArgs, originalArgs, RERECO_ARGS)
        rerecoArgs = deepcopy(rerecoOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(rerecoArgs, requestArgs))

    def testSC_initialize_clone(self):
        """
        Test initialize_clone handling StepChains
        """
        # test overwrite of original values
        requestArgs = {'TimePerEvent': 12.34, 'ConfigCacheID': 'couchid', 'StepChain': 20}
        originalArgs = deepcopy(stepChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, STEPCHAIN_ARGS, STEP_ARGS)
        scArgs = deepcopy(stepChainOriginalArgs)
        scArgs['Step2']['ProcessingVersion'] += 1
        self.assertDictEqual(cloneArgs, updateDict(scArgs, requestArgs))

        # test overwrite of values that are not in the original request
        requestArgs = {'DQMUploadProxy': 'test_file', 'RequestNumEvents': 1, 'DQMSequences': ['a', 'b']}
        originalArgs = deepcopy(stepChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, STEPCHAIN_ARGS, STEP_ARGS)
        scArgs = deepcopy(stepChainOriginalArgs)
        scArgs['Step2']['ProcessingVersion'] += 1
        self.assertDictEqual(cloneArgs, updateDict(scArgs, requestArgs))

        # check whether invalid keys in the original request are dumped
        requestArgs = {'RequestNumEvents': 1, 'DQMSequences': []}
        originalArgs = deepcopy(stepChainOriginalArgs)
        originalArgs.update(Chain='whatever', WMCore={'YouFool': True})
        cloneArgs = initialize_clone(requestArgs, originalArgs, STEPCHAIN_ARGS, STEP_ARGS)
        scArgs = deepcopy(stepChainOriginalArgs)
        scArgs['Step2']['ProcessingVersion'] += 1
        self.assertDictEqual(cloneArgs, updateDict(scArgs, requestArgs))

        # test overwrite of inner Step dictionaries. Make sure ProcVer gets the user provided value
        requestArgs = {'SubRequestType': "RelVal", "Step1": {"BlockWhitelist": ["C"]},
                       "Step2": {"LumisPerJob": 5, "Campaign": "Step2Camp", "ProcessingVersion": 1}}
        originalArgs = deepcopy(stepChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, STEPCHAIN_ARGS, STEP_ARGS)
        scArgs = deepcopy(stepChainOriginalArgs)
        # setting by hand, I assume I better not use the code that is being tested here :-)
        updateDict(scArgs, requestArgs)
        scArgs['Step1'] = {"ConfigCacheID": "blah", "GlobalTag": "PHYS18", "BlockWhitelist": ["C"]}
        scArgs['Step2'] = {"AcquisitionEra": "ACQERA", "ProcessingVersion": 1, "LumisPerJob": 5,
                           "Campaign": "Step2Camp"}
        self.assertDictEqual(cloneArgs, scArgs)

    def testTC_initialize_clone(self):
        """
        Test initialize_clone handling TaskChains
        """
        def updateProcVer(tcArgs):
            tcArgs['Task2']['ProcessingVersion'] += 1
            tcArgs['ProcessingVersion']['name1'] += 1
            tcArgs['ProcessingVersion']['name2'] += 1
            tcArgs['ProcessingVersion']['name3'] += 1

        # test overwrite of original values
        requestArgs = {'ScramArch': "slc6_amd64_gcc630", 'DQMConfigCacheID': None, 'TaskChain': 20}
        originalArgs = deepcopy(taskChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        tcArgs = deepcopy(taskChainOriginalArgs)
        updateProcVer(tcArgs)
        self.assertDictEqual(cloneArgs, updateDict(tcArgs, requestArgs))

        # test overwrite of values that are not in the original request
        requestArgs = {'Task4': {"InputTask": "task222", "TransientOutputModules": ["RAW", "AOD"]},
                       'ConfigCacheID': None, 'EventStreams': 8}
        originalArgs = deepcopy(taskChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        tcArgs = deepcopy(taskChainOriginalArgs)
        updateProcVer(tcArgs)
        self.assertDictEqual(cloneArgs, updateDict(tcArgs, requestArgs))

        # check whether invalid keys in the original request are dumped
        requestArgs = {'RequestPriority': 1, 'DQMSequences': [], "AcquisitionEra": {"T1": "a", "T2": "b"}}
        originalArgs = deepcopy(taskChainOriginalArgs)
        originalArgs.update(ioInput='whatever', WMCore={'YouFool': True}, TotalJobs=123)
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        tcArgs = deepcopy(taskChainOriginalArgs)
        updateProcVer(tcArgs)
        self.assertDictEqual(cloneArgs, updateDict(tcArgs, requestArgs))

        # test overwrite of inner Task dictionaries. Make sure ProcVer gets the user provided value
        requestArgs = {"Task1": {"LumisPerJob": 1, "LumiList": {"202209": [[3, 5]]}},
                       "Task2": {"ProcessingVersion": 10},
                       "Task3": {"TaskName": "newBlah", "TransientOutputModules": []},
                       "PrepID": "prepBlah", "ProcessingVersion": 1}
        originalArgs = deepcopy(taskChainOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, TASKCHAIN_ARGS, TASK_ARGS)
        tcArgs = deepcopy(taskChainOriginalArgs)
        # setting by hand, I assume I better not use the code that is being tested here :-)
        tcArgs['Task1'].update(requestArgs['Task1'])
        tcArgs['Task1']['LumiList'] = {"202205": [[1, 10], [20, 25]], "202209": [[3, 5]]}
        tcArgs['Task2'].update(requestArgs['Task2'])
        tcArgs['Task3'].update(requestArgs['Task3'])
        tcArgs.update({"PrepID": "prepBlah", "ProcessingVersion": 1})
        self.assertDictEqual(cloneArgs, tcArgs)

    def testResub_initialize_clone(self):
        """
        Test the basics of initialize_clone. It cannot increment the ProcessingVersion
        """
        # test overwrite of original values
        requestArgs = {'Multicore': 1, 'ScramArch': 'slc6_amd64_gcc481', 'ProcessingVersion': 10}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))

        # test overwrite of values that are not in the original request
        requestArgs = {'DQMUploadProxy': 'test_file', 'RequestNumEvents': 1, 'DQMSequences': ['a', 'b']}
        originalArgs = deepcopy(mcOriginalArgs)
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))

        # check whether invalid keys in the original request are dumped
        requestArgs = {'RequestNumEvents': 1, 'DQMSequences': []}
        originalArgs = deepcopy(mcOriginalArgs)
        originalArgs.update(badJob='whatever', WMCore={'YouFool': True})
        cloneArgs = initialize_clone(requestArgs, originalArgs, MC_ARGS)
        mcArgs = deepcopy(mcOriginalArgs)
        self.assertDictEqual(cloneArgs, updateDict(mcArgs, requestArgs))


if __name__ == '__main__':
    unittest.main()
