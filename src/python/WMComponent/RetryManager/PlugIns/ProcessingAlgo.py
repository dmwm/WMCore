#!/bin/env python


"""
_LinearAlgo_

Straight lines to the end
"""
import os.path
import logging

from WMCore.FwkJobReport.Report                     import Report
from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase

class ProcessingAlgo(RetryAlgoBase):
    """
    _ProcessingAlgo_

    Test jobs for two specific error conditions:

    If job has an exit code within a list of exit codes, only retry once
      set by ProcessingAlgoOneMoreErrorCodes
    If a job ran for over 24 hours, only retry once
      set by ProcessingAlgoMaxRuntime

    This is done by setting the retry_count to be what we think is the maxRetry count - 1
    """

    def __init__(self, config):

        # Init basics
        RetryAlgoBase.__init__(self, config)

        self.maxRetries = 3

        self.maxRunTime = self.getAlgoParam('default', 'MaxRunTime', 24 * 3600)
        self.exitCodes  = self.getAlgoParam('default', 'OneMoreErrorCodes', [])

        # Try to get the actual number of max retries, but don't mind
        # if this is a test implementation without the full config
        try:
            self.maxRetries = self.config.ErrorHandler.maxRetries
        except:
            logging.debug("No ErrorHandler component passed to RetryManager.ProcessingAlgo - hope this is a test.")
            pass

        return

    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """

        if cooloffType == 'create' or cooloffType == 'submit':
            # Can't really do anything with these: resubmit
            return True

        # Run this to get the errors in the actual job
        try:
            report     = Report()
            reportPath = os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count'])
            report.load(reportPath)
        except:
            # If we're here, then the FWJR doesn't exist.
            # Give up, run it again
            return True

        # Set oneMore flag to be False
        oneMore = False

        # Find startTime, stopTime
        times = report.getFirstStartLastStop()
        startTime = times['startTime']
        stopTime  = times['stopTime']

        if startTime == None or stopTime == None:
            # Well, then we have a problem.
            # There is something very wrong with this job, nevertheless we don't know what it is.
            # Rerun, and hope the times get written the next time around.
            logging.error("No start, stop times for steps")
            return True

        if stopTime - startTime > self.maxRunTime:
            logging.error("Job only allowed to run one more time due to ProcessingAlgo.maxRunTime")
            oneMore = True

        if report.getExitCode() in self.exitCodes:
            logging.error("Job only allowed to run one more time due to ProcessingAlgo.exitCodes")
            oneMore = True


        # Reset the retry time
        if oneMore:
            job['retry_count'] = max(self.maxRetries - 1, job['retry_count'])
            job.save()
            # Hope this gets passed back by reference

        return True
