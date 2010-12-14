#!/bin/env python


"""
_DefaultRetryAlgo_

This is the default.  It's alarmingly simple.
"""

import time
import datetime
import logging
import threading

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class DefaultRetryAlgo(RetryAlgoBase):
    """
    _DefaultRetryAlgo_

    This is the simple 'wait a bit' cooloff algo
    """


    def isReady(self, job, jobType):
        """
        Actual function that does the work

        """

        # Get the cooloff time
        cooloffTime = self.config.RetryManager.coolOffTime.get(jobType.lower(), None)
        
        if not cooloffTime:
            logging.error('Unknown cooloffTime for type %s: passing' %(type))
            return

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False
