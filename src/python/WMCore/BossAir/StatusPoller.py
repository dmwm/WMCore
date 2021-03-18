#!/usr/bin/env python


"""
StatusPoller

Possible non-production poller prototype for
JobStatusAir
"""

from future.utils import listvalues

import time
import logging
import threading
from collections import defaultdict

from Utils.IteratorTools import flattenList
from Utils.Timers import timeFunction
from WMCore.WMException                    import WMException
from WMCore.WMExceptions                   import WM_JOB_ERROR_CODES
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.BossAir.BossAirAPI             import BossAirAPI

class StatusPollerException(WMException):
    """
    _StatusPollerException_

    Exception class for StatusPoller errors.
    """

class StatusPoller(BaseWorkerThread):
    """
    _StatusPoller_

    Prototype for polling for
    JobStatusAir
    """

    def __init__(self, config):
        """
        __init__

        Set up the caching and other objects
        """
        self.config = config
        BaseWorkerThread.__init__(self)

        self.cachedJobs = []

        self.bossAir = BossAirAPI(config=config)

        # With no timeouts, nothing ever happens
        # Otherwise we expect a dictionary with the keys representing
        # the states and the values the timeouts.
        self.timeouts = getattr(config.JobStatusLite, 'stateTimeouts')

        return

    @timeFunction
    def algorithm(self, parameters=None):
        """
        _algorithm_

        Handle any exceptions with the actual code
        """
        myThread = threading.currentThread()
        try:
            logging.info("Running job status poller algorithm...")
            self.checkStatus()
        except WMException as ex:
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollbackForError()
            raise
        except Exception as ex:
            msg = "Unhandled error in statusPoller"
            msg += str(ex)
            logging.exception(msg)
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollbackForError()
            raise StatusPollerException(msg)

        return

    def checkStatus(self):
        """
        _checkStatus_

        Run the BossAir track() function (self-contained)
        and then check for jobs that have timed out.
        """


        runningJobs = self.bossAir.track()

        if len(runningJobs) < 1:
            # Then we have no jobs
            return

        if not self.timeouts:
            # Then we've set ourselves to have no timeouts
            # Get out and stay out
            return

        # Look for jobs that need to be killed
        jobsToKill = defaultdict(list)

        # Now check for timeouts
        for job in runningJobs:
            globalState = job.get('globalState', 'Error')
            statusTime = job.get('status_time', None)
            timeout = self.timeouts.get(globalState, None)
            if statusTime == 0:
                logging.error("Not killing job %i, the status time was zero", job['id'])
                continue
            if timeout and statusTime:
                if time.time() - float(statusTime) > float(timeout):
                    # Timeout status is used by JobTracker to fail jobs in WMBS database
                    logging.info("Killing job %i because it has exceeded timeout for status '%s'", job['id'], globalState)
                    job['status'] = 'Timeout'
                    jobsToKill[globalState].append(job)

        timeOutCodeMap = {"Running": 71304, "Pending": 71305, "Error": 71306}
        # We need to show that the jobs are in state timeout
        # and then kill them.
        jobsToKillList = flattenList(listvalues(jobsToKill))
        myThread = threading.currentThread()
        myThread.transaction.begin()
        self.bossAir.update(jobs=jobsToKillList)
        for preJobStatus in jobsToKill:
            eCode = timeOutCodeMap.get(preJobStatus, 71307) # it shouldn't have 71307 (states should be among Running, Pending, Error)
            self.bossAir.kill(jobs=jobsToKill[preJobStatus], killMsg=WM_JOB_ERROR_CODES[eCode], errorCode=eCode)
        myThread.transaction.commit()

        return

    def terminate(self, params):
        """
        _terminate_

        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
