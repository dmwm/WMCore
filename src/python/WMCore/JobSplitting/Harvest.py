#!/usr/bin/env python
"""
_Harvest_

"""
import threading
import os

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory


class Harvest(JobFactory):
    """
    _Harvest_

    Create jobs to process all files in a fileset. A job will not be created
    until the previous job (if there is one) has been completed and there
    are available (new) files in the fileset.

    Under normal circumstances runs only once as no new files should be
    added to the fileset after it has been closed.

    Note that the period here refers to the amount of time between the end of a
    job and the creation of a new job.

    """
    def getRunSet(self, fileInfo):

        runSet = set()
        if len(fileInfo.getRuns()) == 1:
            run = fileInfo.getRuns().pop()
            runSet.add(run.run)
        else:
            for Run in fileInfo.getRuns():
                runSet.add(Run.run)

        return frozenset(runSet)

    def createJobsLocationWise(self, fileset):

        myThread = threading.currentThread()
        fileset.loadData(parentage = 0)
        allFiles = fileset.getFiles()
        if fileset.open:
            harvestType = 'Periodic-Harvest'
        else:
            harvestType = 'EndOfRun-Harvest'

        # sort by location and run
        locationDict = {}
        runDict = {}
        for fileInfo in allFiles:

            locSet = frozenset(fileInfo['locations'])
            runSet = self.getRunSet(fileInfo)

            if len(locSet) == 0:
                msg = "File %s has no locations!" % fileInfo['lfn']
                myThread.logger.error(msg)
            if len(runSet) == 0:
                msg = "File %s has no run information!" % fileInfo['lfn']
                myThread.logger.error(msg)

            # Populate a dictionary with [location][run] so we can split jobs according to those different combinations
            for location in locSet:
                if location not in locationDict.keys():
                    locationDict[location] = {}
                for run in runSet:
                    if run in locationDict[location].keys():
                        locationDict[location][run].append(fileInfo)
                    else:
                        locationDict[location][run] = [fileInfo]

        # create separate jobs for different locations
        self.newGroup()
        jobCount = 0
        baseName = makeUUID()
        self.newGroup()

        for location in locationDict.keys():
            for run in locationDict[location].keys():
                # Should create at least one job for every location/run, putting this here will do
                jobCount += 1
                self.newJob(name = "%s-%s-%i" % (baseName, harvestType, jobCount))
                for f in locationDict[location][run]:
                    self.currentJob.addFile(f)

                #Check for proxy and ship it in the job if available
                if 'X509_USER_PROXY' in os.environ:
                    self.currentJob['proxyPath'] = os.environ['X509_USER_PROXY']

        return

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        """
        myThread = threading.currentThread()

        periodicInterval = kwargs.get("periodic_harvest_interval", None)

        fileset = self.subscription.getFileset()
        fileset.load()

        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        releasePeriodicJobDAO = daoFactory(classname = "JobSplitting.ReleasePeriodicJob")
        acquiredFilesCountDAO = daoFactory(classname = "Subscriptions.AcquiredFilesCount")

        # If fileset is open go for periodic, otherwise go to EndOfRun
        if fileset.open:
            # We need a valid periodic setting, if it doesn't exist, periodic is deactivated
            if not periodicInterval:
                return

            # Here we decide (in the DB) whether we trigger the Periodic job. Logic is
            # * If there are no jobs yet, screw the delay, fire the first job.
            # * If there are jobs before, look at the delay, is past? N: Bail Y: Fire job only if there are new files.
            triggerJob = releasePeriodicJobDAO.execute(subscription = self.subscription["id"], period = periodicInterval)

            if triggerJob:
                self.createJobsLocationWise(fileset)
        else:
            # Here comes all the EndOfRun checks
            filesInProcessing = acquiredFilesCountDAO.execute(subscription = self.subscription["id"])
            if filesInProcessing > 0:
                myThread.logger.debug("There are %d files being processed by previous jobs, not firing EndOfRun now" % filesInProcessing)
                return
            myThread.logger.debug("Creating End of Run harvesting job")
            self.createJobsLocationWise(fileset)

        return
