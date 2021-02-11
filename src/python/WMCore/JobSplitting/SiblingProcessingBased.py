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
from WMCore.Services.UUIDLib import makeUUID
from WMCore.DAOFactory import DAOFactory

class SiblingProcessingBased(JobFactory):
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Run the discovery query and generate jobs if we find enough files.
        """
        # This doesn't use a proxy
        self.grabByProxy = False

        filesPerJob = int(kwargs.get("files_per_job", 10))

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        fileAvail = daoFactory(classname = "Subscriptions.SiblingSubscriptionsComplete")
        completeFiles = fileAvail.execute(self.subscription["id"],
                                          conn = myThread.transaction.conn,
                                          transaction = True)

        self.subscription["fileset"].load()
        if self.subscription["fileset"].open == True:
            filesetClosed = False
        else:
            fileFailed = daoFactory(classname = "Subscriptions.SiblingSubscriptionsFailed")
            fileFailed.execute(self.subscription["id"],
                               self.subscription["fileset"].id,
                               conn = myThread.transaction.conn,
                               transaction = True)
            filesetClosed = True

        fileSites = {}
        foundFiles = []
        for completeFile in completeFiles:
            if completeFile["lfn"] not in foundFiles:
                foundFiles.append(completeFile["lfn"])
            else:
                continue

            if completeFile["pnn"] not in fileSites:
                fileSites[completeFile["pnn"]] = []

            fileSites[completeFile["pnn"]].append(completeFile)

        for siteName in fileSites:
            if len(fileSites[siteName]) < filesPerJob and not filesetClosed:
                continue

            self.newGroup()
            while len(fileSites[siteName]) >= filesPerJob:
                self.newJob(name = makeUUID())
                for jobFile in fileSites[siteName][0:filesPerJob]:
                    newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                                   events = jobFile["events"])
                    newFile["locations"] = set([jobFile["pnn"]])
                    self.currentJob.addFile(newFile)

                fileSites[siteName] = fileSites[siteName][filesPerJob:]

            if filesetClosed and len(fileSites[siteName]) > 0:
                self.newJob(name = makeUUID())
                for jobFile in fileSites[siteName]:
                    newFile = File(id = jobFile["id"], lfn = jobFile["lfn"],
                                   events = jobFile["events"])
                    newFile["locations"] = set([jobFile["pnn"]])
                    self.currentJob.addFile(newFile)

        return
