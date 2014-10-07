#!/usr/bin/env python


"""
StatusPoller

Possible non-production poller prototype for
JobStatusAir
"""

import time
import logging
import threading
import traceback

from WMCore.WMException                       import WMException
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.BossAir.BossAirAPI    import BossAirAPI, BossAirException

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

        self.bossAir = BossAirAPI(config = config)

        # With no timeouts, nothing ever happens
        # Otherwise we expect a dictionary with the keys representing
        # the states and the values the timeouts.
        self.timeouts = getattr(config.JobStatusLite, 'stateTimeouts', {})

        # init alert system
        self.initAlerts(compName = "StatusPoller")
        return

    def algorithm(self, parameters = None):
        """
        _algorithm_

        Handle any exceptions with the actual code
        """
        self.checkStatus()
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

        if self.timeouts == {}:
            # Then we've set outself to have no timeouts
            # Get out and stay out
            return

        # Look for jobs that need to be killed
        jobsToKill = []

        # Now check for timeouts
        for job in runningJobs:
            globalState = job.get('globalState', 'Error')
            statusTime  = job.get('status_time', None)
            timeout     = self.timeouts.get(globalState, None)
            if statusTime == 0:
                logging.error("Not killing job %i, the status time was zero" % job['id'])
                continue
            if timeout != None and statusTime != None:
                if time.time() - float(statusTime) > float(timeout):
                    # Then the job needs to be killed.
                    logging.info("Killing job %i because it has exceeded timeout for status %s" % (job['id'], globalState))
                    job['status'] = 'Timeout'
                    jobsToKill.append(job)

        # We need to show that the jobs are in state timeout
        # and then kill them.
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.bossAir.update(jobs = jobsToKill)
            self.bossAir.kill(jobs = jobsToKill, killMsg = "Job killed due to timeout")
        except WMException, ex:
            myThread.transaction.rollbackForError()
            self.sendAlert(6, msg = str(ex))
            raise
        except Exception, ex:
            msg =  "Unhandled error in statusPoller" + str(ex)
            logging.exception(msg)
            self.sendAlert(6, msg = msg)
            myThread.transaction.rollbackForError()
            raise StatusPollerException(msg)
        else:
            myThread.transaction.commit()

        return

    def terminate(self, params):
        """
        _terminate_

        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
