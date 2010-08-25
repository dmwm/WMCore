'''
Created on Dec 28, 2009

@author: evansde
'''
import unittest

from WMCore.FwkJobReport.ReportEmu import ReportEmu
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run


class Test(unittest.TestCase):


    def setUp(self):
        self.workload = newWorkload("Sample")
        self.task = self.workload.newTask("Task1")
        self.step = self.task.makeStep("cmsRun1")
        self.step.setStepType("CMSSW")
        self.task.applyTemplates()
        self.cmssw = self.step.getTypeHelper()
        
        self.cmssw.cmsswSetup("CMSSW_X_Y_Z")
        self.cmssw.addOutputModule(
            "output1", 
             primaryDataset = "PRIMARY",
             processedDataset = "Processed_CMSSW_X_Y_Z_output1_v1",
             dataTier = "TIER",
             lfnBase = "/store/data/AcqEra09/PRIMARY/TIER/Processed_CMSSW_X_Y_Z_output1_v1",
             )
        self.cmssw.addOutputModule( "output2",
             primaryDataset = "PRIMARY",
             processedDataset = "Processed_CMSSW_X_Y_Z_output2_v1",
             dataTier = "TIER",
             lfnBase = "/store/data/AcqEra09/PRIMARY/TIER/Processed_CMSSW_X_Y_Z_output2_v1",            )
        self.cmssw.addOutputModule( 
             "output3",
             primaryDataset = "PRIMARY",
             processedDataset = "Processed_CMSSW_X_Y_Z_output3_v1",
             dataTier = "TIER",
             lfnBase = "/store/data/AcqEra09/PRIMARY/TIER/Processed_CMSSW_X_Y_Z_output3_v1",)
        self.cmssw.addOutputModule( "output4",
             primaryDataset = "PRIMARY",
             processedDataset = "Processed_CMSSW_X_Y_Z_output4_v1",
             dataTier = "TIER",
             lfnBase = "/store/data/AcqEra09/PRIMARY/TIER/Processed_CMSSW_X_Y_Z_output4_v1",)
        
        
        self.job = Job()
        
        self.job['task'] = '/Tier1ReReco/ReReco' 
        self.job['name'] = '/Tier1ReReco/ReReco/6b2f2010-ef1f-11de-960a-0018f34d0d52' 
        self.job['counter'] = 0 
        self.job['id'] = '/Tier1ReReco/ReReco/6b2f1e4e-ef1f-11de-960a-0018f34d0d52'
                    
        run1 = Run(1000000)
        run1.lumis.append(1)
        run1.lumis.append(2)
        run1.lumis.append(3)
        run2 = Run(1000001)
        run2.lumis.append(1)
        run2.lumis.append(2)
        run2.lumis.append(3)
        
        wFile1 = File("/store/data/input1.root", 1000000000, 1000)
        wFile1.addRun(run1)
        
        wFile2 = File("/store/data/input2.root", 1000000000, 1000)
        wFile2.addRun(run2)
        
        self.job.addFile(wFile1)
        self.job.addFile(wFile2)
        
        
        
        
    def tearDown(self):
        pass


    def testName(self):
        pass
    
    def testA(self):
        """instantiation test"""
        try:
            emu = ReportEmu()
        except Exception, ex:
            msg = "Failed to instantiate ReportEmu object\n"
            msg += str(ex)
            self.fail(msg)
            
    def testB(self):
        """invoke emulator test"""
        
        emu = ReportEmu(WMStep = self.cmssw, Job = self.job)
        report = emu()
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()