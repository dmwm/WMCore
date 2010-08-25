#!/usr/bin/python
"""
_FJR_t_

General test for FJR

"""

__revision__ = "$Id: FJR_t.py,v 1.6 2009/10/13 22:43:00 meloam Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "fvlingen@caltech.edu"

import logging
import os
import unittest

from WMCore.FwkJobReport.FJR import FJR
from WMCore.FwkJobReport.FJRParser import readJobReport
from WMCore.FwkJobReport import FJRUtils
from WMQuality.TestInit import TestInit

class FJRTest(unittest.TestCase):
    """
    A test of a generic exception class
    """    
    def setUp(self):
        """
        setup log file output.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.tempDir = self.testInit.generateWorkDir()

        
            
    def tearDown(self):
        """
        nothing to tear down
        """
        
        pass
    
    def testA(self):
        """
        Create job report and its attributes.
        """

        fjr = FJR(name = "test", jobSpecId = "jobSpecId_1")
        fjr.status = 'Go'
        fjr.workflowSpecId = 'workflowSpecId_1'
        fjr.jobType = 'a_job_type'
        fjr.dashboardId = 'dashboard_id'
        fjr.siteDetails = {'key1': 'val1', 'key2': 'val2'}
        fjr.storageStatistics = 'some storage statistics'
        fjr.performance.addCPU('dual core', 'fast', 'a description')
        metrics = {'metric1':'value1', 'metric2':'value2'}
        fjr.performance.addSummary('a_metric_class', **metrics)
        fjr.performance.addModule('a_metric_class', 'a_module_name', **metrics)

        fileInfo_o = fjr.newFile()
        fileInfo_o.addInputFile("/a/pfn/location.root", "/a/lfn/location.root")
        fileInfo_o.addChecksum("checksum_algo", 12345)
        fileInfo_o.addRunAndLumi(1, 2, 3, 4, 5, 6, 7)
        datasetInfo_o = fileInfo_o.newDataset()

        datasetInfo_o['PrimaryDataset'] = "RAW1"
        datasetInfo_o['ProcessedDataset'] = "RECO1"
        datasetInfo_o['ParentDataset'] = "VERY_RAW1"
        datasetInfo_o['ApplicationName'] = "Application1"
        datasetInfo_o['ApplicationProject'] = "CMS PROJECT"
        datasetInfo_o['ApplicationVersion'] = "VERSION 1"
        datasetInfo_o['ApplicationFamily'] = "Family 1"
        datasetInfo_o['DataTier'] = "Datatier 1"
        datasetInfo_o['Conditions'] = "none"
        datasetInfo_o['PSetHash'] = "a long hash"
        datasetInfo_o['InputModuleName'] = "input module name"
        datasetInfo_o['OutputModuleName'] = "output module name"

        

        fileInfo_o = fjr.newInputFile()
        fileInfo_o.addInputFile("/a/pfn/location.root", "/a/lfn/location.root")
        fileInfo_o.addChecksum("checksum_algo", 12345)
        fileInfo_o.addRunAndLumi(1, 2, 3, 4, 5, 6, 7)
        datasetInfo_o = fileInfo_o.newDataset()

        datasetInfo_o['PrimaryDataset'] = "RAW1"
        datasetInfo_o['ProcessedDataset'] = "RECO1"
        datasetInfo_o['ParentDataset'] = "VERY_RAW1"
        datasetInfo_o['ApplicationName'] = "Application1"
        datasetInfo_o['ApplicationProject'] = "CMS PROJECT"
        datasetInfo_o['ApplicationVersion'] = "VERSION 1"
        datasetInfo_o['ApplicationFamily'] = "Family 1"
        datasetInfo_o['DataTier'] = "Datatier 1"
        datasetInfo_o['Conditions'] ="none"
        datasetInfo_o['PSetHash'] = "a long hash"
        datasetInfo_o['InputModuleName'] ="input module name"
        datasetInfo_o['OutputModuleName'] = "output module name"

        fileInfo_o = fjr.newAnalysisFile()

        fjr.addSkippedEvent(4, 100)
        error = fjr.addError(10, 20)
        error['ExitStatus'] = 123
        error['Type'] = 'nasty type'
        error['Status'] = '456'
        fjr.addRemovedFile('/a/removed/lfn/location.root','the.se.name')
        fjr.addUnremovedFile('a/unremoved/lfn/location.root','the.se.name')
        fjr.write(os.path.join(self.tempDir,'fjr1.xml'))

    def testB(self):
        """
        Read job report, and extract information.
        """

        reportLocation = os.path.join(self.tempDir,"fjr1.xml")
        fjr = readJobReport(reportLocation) 
        checksum = FJRUtils.readCksum(reportLocation)
        print('Checksum job report: '+str(checksum))
        filesize = FJRUtils.fileSize(reportLocation)
        print('File size job report: '+str(filesize))
        newReportLocation = os.path.join(self.tempDir,"fjr2.xml")
        print('Merging reports')
        FJRUtils.mergeReports(newReportLocation, reportLocation)

        aFJR = fjr[0]

        #Did we get the checksum out?  How about the dataset?
        for fjrFile in aFJR.files:
            self.assertEqual(fjrFile['Checksum'], '12345')
            ds = fjrFile.dataset[0]
            self.assertEqual(ds['PSetHash'], "a long hash")
            self.assertEqual(ds['ProcessedDataset'], "RECO1")
            self.assertEqual(ds['OutputModuleName'], "output module name")

        return

    def testC(self):
        """
        Read multiple jobReports and measure parsing time
        
        Depends on FJRs in WMComponent_t.DBSBuffer_t
        """

        reportDir = os.path.join(os.getenv('WMCOREBASE'), 'test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports')
        if not os.path.isdir(reportDir):
            print "Could not find test/python/WMComponent_t/DBSBuffer_t/FmwkJobReports"
            print "Aborting this test without error"
            return
        dirlist   = os.listdir(reportDir)

        for report in dirlist:
            if not report.endswith('.xml') or not report.startswith('FrameworkJobReport'):
                continue
            reportLocation = os.path.join(reportDir, report)
            jobReports     = readJobReport(reportLocation)

            for aFJR in jobReports:
                for fjrFile in aFJR.files:
                    ds = fjrFile.dataset[0]
                    self.assertEqual(ds['PSetHash'], None)
                    self.assertEqual(ds['ApplicationName'], 'cmsRun')
                    self.assertEqual(ds['ProcessedDataset'].find('Commissioning08-v1'), 0)
        return


            
if __name__ == "__main__":
    unittest.main()     
