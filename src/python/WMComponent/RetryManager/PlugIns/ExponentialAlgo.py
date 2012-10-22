#!/bin/env python


"""
_ExponentialAlgo_

It keeps getting bigger and bigger
"""

from math import pow

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class ExponentialAlgo(RetryAlgoBase):
    """
    _ExponentialAlgo_

    Delay more each retry by some exponential factor
    """


    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """

        # Get the cooloff time
        baseTimeoutDict = self.getAlgoParam(job['jobType'])
        baseTimeout = baseTimeoutDict.get(cooloffType.lower(), 10)
        cooloffTime = pow(baseTimeout, job['retry_count'])

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False
