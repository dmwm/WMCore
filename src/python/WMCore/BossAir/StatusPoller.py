#!/usr/bin/python


"""
StatusPoller

Possible non-production poller prototype for
JobStatusAir
"""

import time
import logging
import threading
import traceback

from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread


from WMCore.BossAir.BossAirAPI    import BossAirAPI, BossAirException



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

        BaseWorkerThread.__init__(self)

        self.cachedJobs = []

        self.bossAir = BossAirAPI(config = config)

        # With no timeouts, nothing ever happens
        # Otherwise we expect a dictionary with the keys representing
        # the states and the values the timeouts.
        self.timeouts = getattr(config.JobStatus, 'stateTimeouts', {})

        return



    def algorithm(self, parameters = None):
        """
        _algorithm_

        Run the code!
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
            if timeout != None and statusTime != None:
                if time.time() - float(statusTime) > float(timeout):
                    # Then the job needs to be killed.
                    logging.info("Killing job %i because it has exceeded timeout for status %s" % (job['id'], globalState))
                    job['status'] = 'Timeout'
                    jobsToKill.append(job)

        # We need to show that the jobs are in state timeout
        # and then kill them.
        self.bossAir.update(jobs = jobsToKill)
        self.bossAir.kill(jobs = jobsToKill)


        return

    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
                
            
