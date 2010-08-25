#!/usr/bin/env python

"""
_PluginBase_

A base plugin class for the jobSubmitter
"""

import os
import logging
import threading

from WMCore.WMBS.Job import Job

class PluginBase(object):
    """
    _BasicPlugin: Submitter_
    
    'Hey!  This does nothing!'
    """

    def __init__(self, config):
        self.config = config

    def submitJobs(self, jobList, localConfig):
        """
        Overwrite this function with something that works
        """
