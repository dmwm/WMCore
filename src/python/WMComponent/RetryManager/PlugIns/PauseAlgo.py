#!/bin/env python

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase
from WMCore.JobStateMachine.ChangeState import ChangeState

import os
from math import pow
from operator import mod


class PauseAlgo(RetryAlgoBase):
    """
    _PauseAlgo_

	This implements the Paused job algorithm, explanation of the concept in #3114
    """
    def __init__ (self, config):
        RetryAlgoBase.__init__(self, config)
        self.changer = ChangeState(config)

    def isReady(self, job, jobType):
        """
        Actual function that does the work
        """
        #This should come from configuration, pause_count

        pauseCount = self.config.RetryManager.PauseCount

        pauseMap = {
            'createcooloff' :    'createpaused',
            'submitcooloff' :    'submitpaused',
            'jobcooloff'    :    'jobpaused'
        }

    	# Here introduces the SquaredAlgo logic :
        baseTimeout = self.config.RetryManager.coolOffTime.get(jobType.lower(), 10)
        cooloffTime = baseTimeout * pow(job['retry_count'], 2)
        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            retryByTimeOut = True
        else:
            retryByTimeOut = False

        if retryByTimeOut :
            # If reached the pauseCount, we want the job to pause instead of retrying
            if mod(job['retry_count'], pauseCount):
                self.changer.propagate(job, pauseMap[job['state']], job['state'])
                return False
            else:
                return True 
        else:
            return False         
