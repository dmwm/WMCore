#!/bin/env python


"""
CreateRetryAlgo

This is a test retryAlgo to illustrate the process of determining when cooloff has expired.
"""

import time
import datetime
import logging
import threading

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase

class CreateRetryAlgo(RetryAlgoBase):
    """
    A RetryAlgo plugin for jobs in the Create state

    """

    def __init__(self):
        object.__init__(self)
        self.config = None

    def setup(self, config):
        """
        Pass in config (WMFactory too stupid to do so on init)

        """
        self.config = config

    def isReady(self, job):
        """
        Actual function that does the work

        """

        cooloffTime = self.config.RetryManager.coolOffTime.get('create', None)
        if not cooloffTime:
            logging.error('Unknown cooloffTime for type %s: passing' %(type))
            return

        currentTime = self.timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False






