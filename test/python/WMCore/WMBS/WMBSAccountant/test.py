#!/usr/bin/env python
"""
Test case for wmbs
"""


import logging
import unittest

from WMCore.WMBS.WMBSAccountant import WMBSAccountant
#from ProdAgentDB.Config import defaultConfig as dbConfig

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Fileset import Subscription
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow

#TODO: Use same connect string as ProdAgent DB
#self.wmbsAccountant = WMBSAccountant(dbConfig)
accountant = WMBSAccountant.WMBSAccountant({'dbName':'sqlite:///:memory:'}, 
                                           'testSystem', '/tmp')

#in real case will be created by prodagent setup script
#accountant.wmbs.createTables()

#null messanger object
ms = lambda x,y,z: True

class WMBSAccountantTester(unittest.TestCase):
    """
    WMBSAccountant test case's
    """
    
    def setUp(self):
        self.wmbsAccountant = accountant
        self.wmbsAccountant.setMessager(ms)
        self.wmbsAccountant.label = 'testSystem'
        
    def test10NewWorkflow(self):
        """
        Insert new workflow
        """
    
        self.wmbsAccountant.newWorkflow('test_workflow.xml')

        # will throw if not in db
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999',
                                self.wmbsAccountant.label, self.wmbsAccountant.wmbs)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                wmbs=self.wmbsAccountant.wmbs).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                wmbs=self.wmbsAccountant.wmbs).load()
        
        
        subs = self.wmbsAccountant.wmbs.listSubscriptionsOfType('Processing')
        self.assertEqual(len(subs), 1)
        subs = self.wmbsAccountant.wmbs.listSubscriptionsOfType('Merge')
        self.assertEqual(len(subs), 1)

    def test20JobSuccess(self):
        """
        test handling of successful job report
        """
        
        self.wmbsAccountant.jobSuccess('test_fjr_success.xml')
        
        # will throw if not in db
        file = File('/store/unmerged/mc/PreCSA08/GEN-SIM-RAW/STARTUP_V2/6763/6467AF28-F31B-DD11-B1EF-003048772324.root',
                    wmbs=self.wmbsAccountant.wmbs).load()
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999',
                                self.wmbsAccountant.label, self.wmbsAccountant.wmbs)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                wmbs=self.wmbsAccountant.wmbs).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                wmbs=self.wmbsAccountant.wmbs).load()
        #should have 1 file ready for merging
        self.assertEqual(mergeSub.availableFiles(), [file])
        
        
    def test30JobFailure(self):
        """
        test handling of failure job report
        """
        
        self.wmbsAccountant.jobFailure('test_fjr_fail.xml')
    
        # will throw if not in db
        file = File('/store/unmerged/mc/PreCSA08/GEN-SIM-RAW/STARTUP_V2/6763/6467AF28-F31B-DD11-B1EF-003048772324.root',
                    wmbs=self.wmbsAccountant.wmbs).load()
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999',
                                self.wmbsAccountant.label, self.wmbsAccountant.wmbs)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               wmbs=self.wmbsAccountant.wmbs).populate()
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                wmbs=self.wmbsAccountant.wmbs).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                wmbs=self.wmbsAccountant.wmbs).load()
        
        #should have 1 failed file
        self.assertEqual(mergeSub.failedFiles(), [file])
    
    
if __name__ == '__main__':
    unittest.main()