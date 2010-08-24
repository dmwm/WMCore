#!/usr/bin/env python
"""
DBS Buffer handler for JobSuccess event
"""
__all__ = []

__revision__ = "$Id: JobSuccess.py,v 1.10 2008/12/11 20:32:04 afaq Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

#from WMComponent.DBSBuffer.Database.Interface.addToBuffer import AddToBuffer
from WMCore.WMFactory import WMFactory

#import cPickle
import os
import string
import logging
import exceptions

from ProdCommon.FwkJobRep.ReportParser import readJobReport

#TODO: InvalidJobReport will come from DBSInterface or elsewhere
class InvalidJobReport(exceptions.Exception):
  def __init__(self,jobreportfile):
   args="Invalid JobReport file: %s\n"%jobreportfile
   exceptions.Exception.__init__(self, args)
   pass

  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

class JobSuccess(BaseHandler):
    """
    Default handler for create failures.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.

        #print "I am not sure about thread pools here"

        #self.threadpool = ThreadPool(\
        #    "WMComponent.DBSBuffer.Handler.DefaultRunSlave", \
        #    self.component, 'JobSuccess', \
        #    self.component.config.DBSBuffer.maxThreads)

        # this we overload from the base handler

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


    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.

        jobReports = self.readJobReportInfo(payload)
        factory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        addToBuffer=factory.loadObject("AddToBuffer")

        for aFJR in jobReports:
            l=0
            print "\n\n"
            for aFile in aFJR.files:
                if l==0:
                    l=1
                    # This shouldn't be required any more
                    # Dataset is being added with workflow spec
                    for dataset in aFile.dataset:
                        #Heck if it already exists in DBS Buffer
                        addToBuffer.addAlgo(dataset)                    
                        addToBuffer.addDataset(dataset, algoInDBS=0)

                        """
                        print "Dataset Path : " + \
                        "/"+aFile.dataset[0]['PrimaryDataset']+ \
                        "/"+aFile.dataset[0]['ProcessedDataset']+ \
                        "/"+aFile.dataset[0]['DataTier']
                        """

                        #Lets see if ALGO info is present in the dataset
                        #### Lets fake test it
                        pset=aFile['PSetHash']="ABCDEFGHIJKL12345676"
                        print "FAKING PSetHash in file <<<<<<<<<"
                        if aFile['PSetHash'] != None:
                            #Pass in the dataset, that contains all info about Algo
                            # 
                            addToBuffer.updateAlgo(dataset, pset)
                print "\n\nDetermine if there can be more than one datasets in a File and which one to pick here"
                dataset=aFile.dataset[0]
               
		addToBuffer.addFJRFile(aFile) 
                addToBuffer.addFile(aFile, dataset)
                
                """
			    print "\n\n\n"
                print aFile['LFN']
			    print aFile['TotalEvents']
			    print aFile['Size']
			    print aFile.checksums['cksum']
			    print "aFile['Type']='UNKNOWN'"
			    print "aFile['Block']='UNKNOWN'"
			    print aFile.runs
			    print aFile.getLumiSections()
			    print "aFile['inputFiles']"
                """
                
