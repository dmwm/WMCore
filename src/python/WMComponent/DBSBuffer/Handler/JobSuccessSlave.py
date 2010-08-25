#!/usr/bin/env python
"""
DBS Buffer handler for JobSuccess event
"""
__all__ = []

__revision__ = "$Id: JobSuccessSlave.py,v 1.3 2009/07/15 20:41:30 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "mnorman@fnal.gov"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
from WMCore.WMFactory import WMFactory

from WMCore.ThreadPool.ThreadSlave import ThreadSlave

import os
import string
import logging
import exceptions
import time
import threading

from ProdCommon.FwkJobRep.ReportParser import readJobReport

class JobSuccessSlave(ThreadSlave):
    """
    Default handler for create failures: slave version.
    """

    def readJobReportInfo(self, jobReportFile):
        """
        _readJobReportInfo_

        Read the info from jobReport file

        """
        jobReportFile = string.replace(jobReportFile, "file://", "")
        if not os.path.exists(jobReportFile):
            logging.error("JobReport Not Found: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)
        try:
            jobreports = readJobReport(jobReportFile)
        except:
            logging.debug("Invalid JobReport File: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)

        return jobreports


    def __call__(self, parameters):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.

        jobReports = self.readJobReportInfo(parameters)
        myThread = threading.currentThread()
        myThread.transaction.begin()

        payload = parameters #I think this is correct; it's all that I've got to go on
        jobReports = self.readJobReportInfo(payload)

        for aFJR in jobReports:
            for fwjrFile in aFJR.files:
                bufferFile = DBSBufferFile(lfn = fwjrFile["LFN"], size = fwjrFile["Size"],
                                           events = fwjrFile["TotalEvents"],
                                           cksum = fwjrFile["Checksum"])
                
                datasetPath = "/%s/%s/%s" % (fwjrFile.dataset[0]["PrimaryDataset"],
                                             fwjrFile.dataset[0]["ProcessedDataset"],
                                             fwjrFile.dataset[0]["DataTier"])
                bufferFile.setDatasetPath(datasetPath)

                if fwjrFile.dataset[0]["PSetHash"] == None:
                    fwjrFile.dataset[0]["PSetHash"] = "BOGUS"

                myThread.logger.debug("Dataset Path567: %s" % datasetPath)
                myThread.logger.debug("ALGO: %s %s %s %s" % (fwjrFile.dataset[0]["ApplicationName"], fwjrFile.dataset[0]["ApplicationVersion"], fwjrFile.dataset[0]["ApplicationFamily"], fwjrFile.dataset[0]["PSetHash"]))

                bufferFile.setAlgorithm(appName = fwjrFile.dataset[0]["ApplicationName"],
                                        appVer = fwjrFile.dataset[0]["ApplicationVersion"],
                                        appFam = fwjrFile.dataset[0]["ApplicationFamily"],
                                        psetHash = fwjrFile.dataset[0]["PSetHash"])
                
                bufferFile.create()

        myThread.transaction.commit()
        return


        

