#!/usr/bin/env python
"""
_SiblingProcessingBased_

Launch jobs to run over a file once all other subscriptions that process the file
have completed processing it.
"""




import threading
import logging

from WMCore.WMBS.File import File

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class SiblingProcessingBased(JobFactory):
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

        fileSites = {}
        for completeFile in completeFiles:
            if not fileSites.has_key(completeFile["se_name"]):
                fileSites[completeFile["se_name"]] = []

            fileSites[completeFile["se_name"]].append(completeFile)

        for siteName in fileSites.keys():
            if len(fileSites[siteName]) < filesPerJob and not filesetClosed:
                continue

            self.newGroup()
            while len(fileSites[siteName]) >= filesPerJob:
                self.newJob(name = makeUUID())
                for jobFile in fileSites[siteName][0:filesPerJob]:
                    newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                                   events = jobFile["events"])
                    newFile["locations"] = set([jobFile["se_name"]])                
                    self.currentJob.addFile(newFile)
                
                    fileSites[siteName] = fileSites[siteName][filesPerJob:]

            if filesetClosed and len(fileSites[siteName]) > 0:
                self.newJob(name = makeUUID())
                for jobFile in fileSites[siteName]:
                    newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                                   events = jobFile["events"])
                    newFile["locations"] = set([jobFile["se_name"]])
                    self.currentJob.addFile(newFile)            

        return
