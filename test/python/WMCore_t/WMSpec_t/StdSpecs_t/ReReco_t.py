#!/usr/bin/env python
"""
Unittest for WMStep
"""


import unittest
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, getTestArguments


class ReRecoTest(unittest.TestCase):
    
    def setUp(self):
        self.defaultAgrs = getTestArguments()
        
    def testJobSplittingAndWorkQueuePolicyMap(self):
        
        #Test file based
        splitAgrs = {"StdJobSplitAlgo": 'FileBased',
                     "StdJobSplitArgs": {'files_per_job': 1}
                     }
        
        self.defaultAgrs.update(splitAgrs)
        reReco = rerecoWorkload("testWorkload", self.defaultAgrs)
        self.assertEqual(reReco.startPolicyParameters(), 
                         {'policyName': 'Block', 
                          'SliceType': 'NumberOfFiles',
                          'SliceSize': 1})
        
        #Test event based
        splitAgrs = {"StdJobSplitAlgo": 'EventBased',
                     "StdJobSplitArgs": {'events_per_job': 1000}
                     }
        
        self.defaultAgrs.update(splitAgrs)
        reReco = rerecoWorkload("testWorkload", self.defaultAgrs)
        self.assertEqual(reReco.startPolicyParameters(), 
                         {'policyName': 'Block', 
                          'SliceType': 'NumberOfEvents',
                          'SliceSize': 1000})
       
        #Test lumi based
        #TODO currently it has default value of FileBased.
        #This needs to be changed when dbs api for getting number of lumis
        splitAgrs = {"StdJobSplitAlgo": 'LumiBased',
                     "StdJobSplitArgs": {'lumis_per_job': 10}
                     }
        self.defaultAgrs.update(splitAgrs)
        reReco = rerecoWorkload("testWorkload", self.defaultAgrs)
        self.assertEqual(reReco.startPolicyParameters(), 
                         {'policyName': 'Block', 
                          'SliceType': 'NumberOfFiles',
                          'SliceSize': 1})
        
if __name__ == "__main__":
    unittest.main()
