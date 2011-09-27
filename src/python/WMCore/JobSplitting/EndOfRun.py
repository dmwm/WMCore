#!/usr/bin/env python
"""
_EndOfRun_

"""




import time
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class EndOfRun(JobFactory):
    """
    _EndOfRun_

    Create jobs to process all files in a fileset. A job will not be created
    until the previous job (if there is one) has been completed and there
    are available (new) files in the fileset.

    Under normal circumstances runs only once as no new files should be
    added to the fileset after it has been closed.

    Note that the period here refers to the amount of time between the end of a
    job and the creation of a new job.

    """
    def outstandingJobs(self, jobPeriod):
        """
        _outstandingJobs_

        Determine whether or not there are outstanding jobs and whether or not
        enough time has elapsed from the previous job to warrant creating a new
        job.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        stateDAO = daoFactory(classname = "Jobs.NewestStateChangeForSub")
        results = stateDAO.execute(subscription = self.subscription["id"])

        if len(results) > 0:
            for result in results:
                if result["name"] not in ["closeout", "cleanout", "exhausted"]:
                    myThread.logger.debug("EndOfRun: Outstanding jobs, returning...")
                    return True

            stateTime = int(results[0]["state_time"])
            if stateTime + jobPeriod > time.time():
                myThread.logger.debug("EndOfRun: %d seconds until next job..." % \
                                      ((stateTime + jobPeriod) - time.time()))
                return True

        return False

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        """
        myThread = threading.currentThread()

        filesPerJob  = int(kwargs.get("files_per_job", 999999))
        jobPeriod = int(kwargs.get("job_period", 900))
       
        fileset = self.subscription.getFileset()
        fileset.load()

        # If fileset is open do nothing.
        if fileset.open:
            return

        # Wait for enough time after last job completion.
        if self.outstandingJobs(jobPeriod):
            return

        # Do we have available (new) files to run on ?
        availableFiles = self.subscription.availableFiles()
        if len(availableFiles) == 0:
            myThread.logger.debug("EndOfRun: No available files...")
            return

        fileset.loadData()
        allFiles = fileset.getFiles()

        # sort by location
        locationDict = {}
        for fileInfo in allFiles:

            locSet = frozenset(fileInfo['locations'])

            if len(locSet) == 0:
                msg = "File %s has no locations!" % fileInfo['lfn']
                myThread.logger.error(msg)

            if locSet in locationDict.keys():
                locationDict[locSet].append(fileInfo)
            else:
                locationDict[locSet] = [fileInfo]

        # create separate jobs for different locations
        self.newGroup()
        jobCount = 0
        baseName = makeUUID()
        self.newGroup()
        for location in locationDict.keys():
            filesInJob  = 0
            for f in locationDict[location]:
                if filesInJob == 0 or filesInJob >= filesPerJob:
                    self.newJob(name = "%s-endofrun-%i" % (baseName, jobCount))
                    filesInJob = 0
                    jobCount += 1
                self.currentJob.addFile(f)
                filesInJob += 1

        return
