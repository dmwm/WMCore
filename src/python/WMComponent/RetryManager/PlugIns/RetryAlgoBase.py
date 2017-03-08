#!/bin/env python


"""
RetryAlgoBase

This is the base class for Retry Algos
"""

from builtins import object
import time
import datetime
import logging

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

    def isReady(self, job, cooloffType):
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

    def getAlgoParam(self, jobType, param = 'coolOffTime', defaultReturn = {}):
        """
        _getAlgoParam_

        Get a parameter from the config for the current algorithm and given
        job type
        """
        pluginName = self.__class__.__name__
        pluginArgs = getattr(self.config.RetryManager, pluginName)

        if hasattr(pluginArgs, jobType):
            algoParams = getattr(pluginArgs, jobType)
        else:
            algoParams = pluginArgs.default

        if hasattr(algoParams, param):
            return getattr(algoParams, param)
        else:
            logging.error("No %s for %s algorithm and %s job type" % (param, pluginName, jobType))
            return defaultReturn
