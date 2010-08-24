#!/usr/bin/env python
"""
DBS Buffer handler for BufferSuccess event
"""
__all__ = []

__revision__ = "$Id: BufferSuccess.py,v 1.1 2008/10/08 21:19:34 afaq Exp $"
__version__ = "$Reivison: $"
__author__ = "anzar@fnal.gov"

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile


import cPickle
import os
import string
import logging

from ProdCommon.FwkJobRep.ReportParser import readJobReport




class BufferSuccess(BaseHandler):
    """
    Default handler for create failures.
    """


    """
    def __init__(self):
	BaseHandler.__init__(self)
	print "THIS is Called"
    """


    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.

	#print "I am not sure about thread pools here"

        #self.threadpool = ThreadPool(\
        #    "WMComponent.DBSBuffer.Handler.DefaultRunSlave", \
        #    self.component, 'BufferSuccess', \
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

	print event, payload
	print event + " ::::::: Handled"

        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSBuffer/DefaultConfig.py'))
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")


	print "FJR is received in %s" %config.General.workDir
	fjr_file = os.path.join(config.General.workDir, "fjr.pck")
	# Unpickle
	fptr = open(fjr_file, 'r')
	fjr_unpck = cPickle.load(fptr)
	fptr.close()


	fjr = os.path.join(config.General.workDir, "fjr_uuid_here.xml")
	
	fjr_file=open(fjr, 'w')
	fjr_file.write(fjr_unpck)
	fjr_file.close()

	print "This is UN-NECESSARy STEP of writting the file to disk first and then reading from there"
	print "Will investigate if readJobReport can handle an open file instead"
	
	
	# 
	
	jobReportInfo = self.readJobReportInfo(fjr)[0]

	import pdb
	pdb.set_trace()

	for aFile in jobReportInfo.files:
		print aFile
		print aFile.dataset

	print "Tu ney kia kar dala...."

	#fjrPickle = open("fjr.pck", 'r')
	#cPickle.dump(str(open(payload, 'r').read()), fjrPickle)
	#fjrPickle.close()

        #self.threadpool.enqueue(event, payload)


