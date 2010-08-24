
import logging
import threading

from WMCore.WMFactory import WMFactory

class Default:

    def __init__(self):
        self.classads = None
        self.cooloff = "00:1:00"
        self.factory = WMFactory('factory','')
        myThread = threading.currentThread()
        self.queries = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.Sites')
        # the tracker db is eithere hooked up to the placebo one (if run in-situ) or the real one.
        # we keep this none so it gives an error when not properly assigned.
        self.trackerDB = None

    def initialise(self):
        """
        _initialise_

        """
        pass

    def updateSubmitted(self, submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("JobEmulator: Track Count: %s" % len(submitted))

        jobInfos = self.queries.jobsById(submitted)
        remove = []
        for jobInfo in jobInfos:
            jobState = jobInfo[3]
            if jobState == "new":
                self.trackerDB.jobRunning(jobInfo[0])
            elif jobState == "failed":
                self.trackerDB.jobFailed(jobInfo[0])
                remove.append(jobInfo[0])
            elif jobState == "finished":
                self.trackerDB.jobComplete(jobInfo[0])
                remove.append(jobInfo[0])
            else:
                logging.error("Unknown job state: %s" % jobState)
        self.queries.removeJob(remove)


    def updateRunning(self, running):
        """
        _updateRunning_

        Check on Running Job

        """
        # logic is same as updateSubmitted.
        self.updateSubmitted(running)

    def updateComplete(self, complete):
        """
        _updateComplete_

        Take any required action on completion.

        Note: Do not publish these to the PA as success/failure, that
        is handled by the component itself

        """
        if len(complete) == 0:
            return
        summary = "Jobs Completed:\n"
        for compId in complete:
            summary += " -> %s\n" % compId
        logging.info(summary)
        return

    def updateFailed(self, failed):
        """
        _updateFailed_

        Take any required action for failed jobs on completion

        """
        pass

    def kill(self, toKill):
        """
        _kill_

        """
        pass

    def cleanup(self):
        """
        _cleanup_

        """
        pass

