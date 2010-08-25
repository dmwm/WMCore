#!/usr/bin/env python
"""
DBS Buffer handler for JobSuccess event
"""
__all__ = []

__revision__ = "$Id: JobSuccessSlave.py,v 1.2 2009/06/10 16:29:16 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

#from WMComponent.DBSBuffer.Database.Interface.addToBuffer import AddToBuffer
from WMCore.WMFactory import WMFactory

#from WMComponent.ErrorHandler.Handler.DefaultSlave import DefaultSlave
from WMCore.ThreadPool.ThreadSlave import ThreadSlave

#import cPickle
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

    def readJobReportInfo(self,jobReportFile):
        """
        _readJobReportInfo_

        Read the info from jobReport file

        """

        jobReportFile=string.replace(jobReportFile,'file://','')
        if not os.path.exists(jobReportFile):
            logging.error("JobReport Not Found: %s" %jobReportFile)
            raise InvalidJobReport(jobReportFile)
        try:
            jobreports=readJobReport(jobReportFile)
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

        myThread = threading.currentThread()
        myThread.transaction.begin()



        payload = parameters #I think this is correct; it's all that I've got to go on
        jobReports = self.readJobReportInfo(payload)
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        addToBuffer=factory.loadObject("AddToBuffer")

        for aFJR in jobReports:
            for aFile in aFJR.files:
                # This shouldn't be required any more
                # Dataset is being added with workflow spec
                for dataset in aFile.dataset:
                    addToBuffer.addAlgo(dataset)
                    addToBuffer.addDataset(dataset, algoInDBS=0)
                    addToBuffer.updateDSFileCount(dataset)
                    #Lets see if ALGO info is present in the dataset
                    #### Lets fake test it
                    #pset=aFile['PSetHash']="ABCDEFGHIJKL12345676"
                    #print "FAKING PSetHash in file <<<<<<<<<"
                    if aFile.has_key('PSetHash') and aFile['PSetHash'] != None:
                       #Pass in the dataset, that contains all info about Algo
                       # 
                       addToBuffer.updateAlgo(dataset, pset)
		dataset=aFile.dataset[0]
		datasetPath='/'+dataset['PrimaryDataset']+'/'+ \
                                        dataset['ProcessedDataset']+'/'+ \
                                        dataset['DataTier']
		addToBuffer.addFile(aFile, datasetPath)

        myThread.transaction.commit()

        return


        

