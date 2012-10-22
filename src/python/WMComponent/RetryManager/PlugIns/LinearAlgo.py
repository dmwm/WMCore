#!/bin/env python


"""
_LinearAlgo_

Straight lines to the end
"""

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class LinearAlgo(RetryAlgoBase):
    """
    _LinearAlgo_

    Make a linear backoff for the jobs
    """


    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """

        # Get the cooloff time
        baseTimeoutDict = self.getAlgoParam(job['jobType'])
        baseTimeout = baseTimeoutDict.get(cooloffType.lower(), 10)
        cooloffTime = baseTimeout * job['retry_count']

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False
