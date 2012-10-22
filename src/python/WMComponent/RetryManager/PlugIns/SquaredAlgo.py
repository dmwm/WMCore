#!/bin/env python


"""
_SquaredAlgo_

Square the bottom and away we go
"""

from math import pow

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class SquaredAlgo(RetryAlgoBase):
    """
    _SquaredAlgo_

    Delay retry by a straight square function
    """


    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """

        # Get the cooloff time
        baseTimeoutDict = self.getAlgoParam(job['jobType'])
        baseTimeout = baseTimeoutDict.get(cooloffType.lower(), 10)
        cooloffTime = baseTimeout * pow(job['retry_count'], 2)

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False
