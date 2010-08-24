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
ms = lambda x,y,z: None

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

        # will throw if not in db
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999',
                            'StuartTest-RelValSingleElectronPt10-999',
                self.wmbsAccountant.label, dbfactory=self.wmbsAccountant.db)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        # create sub
        self.wmbsAccountant.createSubscription('test_workflow.xml')
        
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                dbfactory=self.wmbsAccountant.db).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                dbfactory=self.wmbsAccountant.db).load()
        
        
        subs = unmergedDataset.subscriptions('Processing')
        self.assertEqual(len(subs), 1)
        subs = mergedDataset.subscriptions('Merge')
        self.assertEqual(len(subs), 1)

    def test20JobSuccess(self):
        """
        test handling of successful job report
        """
        
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999', 'StuartTest-RelValSingleElectronPt10-999',
                                self.wmbsAccountant.label, dbfactory=self.wmbsAccountant.db)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                dbfactory=self.wmbsAccountant.db).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                dbfactory=self.wmbsAccountant.db).load()
        
        self.wmbsAccountant.handleJobReport('test_fjr_success.xml', [])
        
        # will throw if not in db
        file = File('/store/unmerged/mc/PreCSA08/GEN-SIM-RAW/STARTUP_V2/6763/6467AF28-F31B-DD11-B1EF-003048772324.root',
                    dbfactory=self.wmbsAccountant.db).load()
        
        #should have 1 file ready for merging
        self.assertEqual(mergeSub.availableFiles(), [file])
        
        
    def test30JobFailure(self):
        """
        test handling of failure job report
        """
        
        self.wmbsAccountant.handleJobReport('test_fjr_fail.xml', [])
    
        # will throw if not in db
        file = File('/store/unmerged/mc/PreCSA08/GEN-SIM-RAW/STARTUP_V2/6763/6467AF28-F31B-DD11-B1EF-003048772324.root',
                    dbfactory=self.wmbsAccountant.db).load()
        workflow = Workflow('StuartTest-RelValSingleElectronPt10-999', 'StuartTest-RelValSingleElectronPt10-999',
                                self.wmbsAccountant.label, dbfactory=self.wmbsAccountant.db)
        inputDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-RelVal-1209247429-IDEAL_V1-2nd-IDEAL_V1/GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        unmergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999-unmerged/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        mergedDataset = Fileset('/RelValSingleElectronPt10/CMSSW_2_0_5-StuartTest-999/GEN-SIM-DIGI-RECO',
                               dbfactory=self.wmbsAccountant.db).populate()
        procSub = Subscription(inputDataset, workflow, type='Processing',
                                dbfactory=self.wmbsAccountant.db).load()
        mergeSub = Subscription(unmergedDataset, workflow, type='Merge',
                                dbfactory=self.wmbsAccountant.db).load()
        
        #should have 1 failed file
        self.assertEqual(mergeSub.failedFiles(), [file])
    
    
if __name__ == '__main__':
    unittest.main()