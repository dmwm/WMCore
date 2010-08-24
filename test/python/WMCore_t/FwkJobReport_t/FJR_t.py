#!/usr/bin/python
"""
_FJR_t_

General test for FJR

"""

__revision__ = "$Id: FJR_t.py,v 1.1 2008/10/08 15:34:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging
import os
import unittest

from WMCore.FwkJobReport.FJR import FJR
from WMCore.FwkJobReport.FJRParser import readJobReport
from WMCore.FwkJobReport import FJRUtils

class FJRTest(unittest.TestCase):
    """
    A test of a generic exception class
    """    
    def setUp(self):
        """
        setup log file output.
        """
        logging.basicConfig(level=logging.NOTSET,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')
        
        self.logger = logging.getLogger('FJRTest')
        
            
    def tearDown(self):
        """
        nothing to tear down
        """
        
        pass
    
    def testA(self):
        fjr = FJR(name = "test", jobSpecId = "jobSpecId_1")
        fjr.status = 'Go'
        fjr.workflowSpecId = 'workflowSpecId_1'
        fjr.jobType = 'a_job_type'
        fjr.dashboardId = 'dashboard_id'
        fjr.siteDetails = {'key1': 'val1', 'key2': 'val2'}
        fjr.storageStatistics = 'some storage statistics'
        fjr.performance.addCPU('dual core','fast','a description')
        metrics = {'metric1':'value1','metric2':'value2'}
        fjr.performance.addSummary('a_metric_class', **metrics)
        fjr.performance.addModule('a_metric_class','a_module_name', **metrics)

        fileInfo_o = fjr.newFile()
        fileInfo_o.addInputFile("/a/pfn/location.root", "/a/lfn/location.root")
        fileInfo_o.addChecksum("checksum_algo", 12345)
        fileInfo_o.addRunAndLumi(1,2,3,4,5,6,7)
        datasetInfo_o = fileInfo_o.newDataset()

        datasetInfo_o['PrimaryDataset'] = "RAW1"
        datasetInfo_o['ProcessedDataset'] = "RECO1"
        datasetInfo_o['ParentDataset'] = "VERY_RAW1"
        datasetInfo_o['ApplicationName'] = "Application1"
        datasetInfo_o['ApplicationProject'] ="CMS PROJECT"
        datasetInfo_o['ApplicationVersion'] ="VERSION 1"
        datasetInfo_o['ApplicationFamily'] = "Family 1"
        datasetInfo_o['DataTier'] = "Datatier 1"
        datasetInfo_o['Conditions'] ="none"
        datasetInfo_o['PSetHash'] = "a long hash"
        datasetInfo_o['InputModuleName'] ="input module name"
        datasetInfo_o['OutputModuleName'] = "output module name"

        

        fileInfo_o = fjr.newInputFile()
        fileInfo_o.addInputFile("/a/pfn/location.root", "/a/lfn/location.root")
        fileInfo_o.addChecksum("checksum_algo", 12345)
        fileInfo_o.addRunAndLumi(1,2,3,4,5,6,7)
        datasetInfo_o = fileInfo_o.newDataset()

        datasetInfo_o['PrimaryDataset'] = "RAW1"
        datasetInfo_o['ProcessedDataset'] = "RECO1"
        datasetInfo_o['ParentDataset'] = "VERY_RAW1"
        datasetInfo_o['ApplicationName'] = "Application1"
        datasetInfo_o['ApplicationProject'] ="CMS PROJECT"
        datasetInfo_o['ApplicationVersion'] ="VERSION 1"
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
        fjr.write(os.path.join(os.getenv("TESTDIR"),'fjr1.xml'))

    def testB(self):
        reportLocation = os.path.join(os.getenv("TESTDIR"),"fjr1.xml")
        fjr = readJobReport(reportLocation) 
        checksum = FJRUtils.readCksum(reportLocation)
        print('Checksum job report: '+str(checksum))
        filesize = FJRUtils.fileSize(reportLocation)
        print('File size job report: '+str(filesize))
        newReportLocation = os.path.join(os.getenv("TESTDIR"),"fjr2.xml")
        print('Merging reports')
        FJRUtils.mergeReports(newReportLocation, reportLocation)

    def runTest(self):
        self.testA()
        self.testB()
            
if __name__ == "__main__":
    unittest.main()     
