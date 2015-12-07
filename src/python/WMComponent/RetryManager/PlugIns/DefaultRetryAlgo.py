#!/bin/env python


"""
_DefaultRetryAlgo_

This is the default.  It's alarmingly simple.
"""

import logging

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class DefaultRetryAlgo(RetryAlgoBase):
    """
    _DefaultRetryAlgo_

    This is the simple 'wait a bit' cooloff algo
    """


    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """

        # Get the cooloff time
        cooloffDict = self.getAlgoParam(job['jobType'])
        cooloffTime = cooloffDict.get(cooloffType.lower(), None)

        if not cooloffTime:
            logging.error('Unknown cooloffTime for type %s: passing' %(type))
            return False

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False
