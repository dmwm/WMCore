#!/bin/env python


"""
RetryAlgoBase

This is the base class for Retry Algos
"""

import time
import datetime

class RetryAlgoBase(object):
    """
    A RetryAlgo plugin for jobs in the Create state

    """

    def __init__(self, config):
        object.__init__(self)
        self.config = config

    def setup(self, config):
        """
        Pass in config (WMFactory too stupid to do so on init)

        """
        self.config = config

    def isReady(self, job, jobType):
        """
        Actual function that does the work

        """

        pass

    def convertdatetime(self, t):
        return int(time.mktime(t.timetuple()))
          
    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return self.convertdatetime(t)
