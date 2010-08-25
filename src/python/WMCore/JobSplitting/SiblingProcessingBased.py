#!/usr/bin/env python
"""
_SiblingProcessingBased_

Launch jobs to run over a file once all other subscriptions that process the file
have completed processing it.
"""

__revision__ = "$Id: SiblingProcessingBased.py,v 1.3 2010/04/23 16:43:25 sfoulkes Exp $"
__version__  = "$Revision: 1.3 $"

import threading
import logging

from WMCore.WMBS.File import File

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class SiblingProcessingBased(JobFactory):
    """
    _SiblingProcessingBased_
    
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Run the discovery query and generate jobs if we find enough files.
        """
        filesPerJob = int(kwargs.get("files_per_job", 10))

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        fileAvail = daoFactory(classname = "Subscriptions.SiblingSubscriptionsComplete")
        completeFiles = fileAvail.execute(self.subscription["id"],
                                          self.subscription["fileset"].id)

        self.subscription["fileset"].load()
        if self.subscription["fileset"].open == True:
            filesetClosed = False
        else:
            filesetClosed = True

        if len(completeFiles) < filesPerJob and not filesetClosed:
            return

        self.newGroup()
        while len(completeFiles) >= filesPerJob:
            self.newJob(name = makeUUID())
            for jobFile in completeFiles[0:filesPerJob]:
                newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                               events = jobFile["events"])
                self.currentJob.addFile(newFile)
                
            completeFiles = completeFiles[filesPerJob:]

        if filesetClosed and len(completeFiles) > 0:
            self.newJob(name = makeUUID())
            for jobFile in completeFiles:
                newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                               events = jobFile["events"])
                self.currentJob.addFile(newFile)            

        return
