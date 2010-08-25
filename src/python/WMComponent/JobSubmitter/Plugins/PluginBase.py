#!/usr/bin/env python

"""
_PluginBase_

A base plugin class for the jobSubmitter
"""

import os
import logging
import threading

from WMCore.WMBS.Job import Job

from WMCore.Agent.Configuration           import Configuration
from WMCore.JobStateMachine.ChangeState   import ChangeState

class PluginBase(object):
    """
    _BasicPlugin: Submitter_
    
    'Hey!  This does nothing!'
    """

    def __init__(self, config):



        csConfig = Configuration()
        csConfig.section_("JobStateMachine")
        csConfig.JobStateMachine.couchurl      = config["couchURL"]
        csConfig.JobStateMachine.couch_retries = config["defaultRetries"]
        csConfig.JobStateMachine.couchDBName   = config["couchDBName"]


        self.changeState = ChangeState(csConfig)
        
        self.config = config

    def submitJobs(self, jobList, localConfig):
        """
        Overwrite this function with something that works
        """


    def passJobs(self, jobList):
        """
        _passJobs_
        
        Run the jobs through ChangeState
        """

        self.changeState.propagate(jobList, 'executing',    'created')

        return


    def failJobs(self, jobList):
        """
        _failJobs_

        Fail the jobs that failed in the submitter
        """

        self.changeState.propagate(jobList, 'submitfailed', 'created')

        return
    
