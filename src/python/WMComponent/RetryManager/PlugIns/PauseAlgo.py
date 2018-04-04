#!/bin/env python
"""
_PauseAlgo_
"""
import os.path
import logging
from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.FwkJobReport.Report import Report


class PauseAlgo(RetryAlgoBase):
    """
    _PauseAlgo_

        This implements the Paused job algorithm, explanation of the concept in #3114
    """

    def __init__(self, config):
        RetryAlgoBase.__init__(self, config)
        self.changer = ChangeState(config)

    def isReady(self, job, cooloffType):
        """
        Actual function that does the work
        """
        # This should come from configuration, pause_count

        pauseCount = self.getAlgoParam(job['jobType'], param='pauseCount', defaultReturn=3)

        pauseMap = {
            'createcooloff': 'createpaused',
            'submitcooloff': 'submitpaused',
            'jobcooloff': 'jobpaused'
            }

        # Setting a pauseCount depending on job exit code
        if job['state'] == 'jobcooloff':
            exitCodes = self.getAlgoParam(job['jobType'], 'retryErrorCodes', {})
            if exitCodes:
                report = Report()
                reportPath = os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count'])
                try:
                    report.load(reportPath)
                    jobExitCode = report.getExitCode()
                    # If the jobExitCode is configured, set the respective pauseCount for the job.
                    if jobExitCode in exitCodes:
                        retryByTimeOut = True
                        pauseCount = exitCodes[jobExitCode]
                except IOError as ex:
                    msg = "Error loading report %s\n" % (reportPath)
                    msg += str(ex)
                    logging.warning(msg)

        # Here introduces the SquaredAlgo logic :
        baseTimeoutDict = self.getAlgoParam(job['jobType'])
        baseTimeout = baseTimeoutDict.get(cooloffType.lower(), 10)
        cooloffTime = baseTimeout * pow(job['retry_count'], 2)
        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            retryByTimeOut = True
        else:
            retryByTimeOut = False

        if retryByTimeOut:
            # If reached the pauseCount, we want the job to pause instead of retrying
            if pauseCount == 0:
                self.changer.propagate(job, pauseMap[job['state']], job['state'], updatesummary=True)
                return False
            elif job['retry_count'] > 0 and not job['retry_count'] % pauseCount:
                self.changer.propagate(job, pauseMap[job['state']], job['state'], updatesummary=True)
                return False
            else:
                return True
        else:
            return False
