#!/usr/bin/env python

"""
VanillaCondorPlugin

BossAir plugin for vanilla condor
"""
import os.path
import logging

from WMCore.BossAir.Plugins.CondorPlugin import CondorPlugin
from WMCore.Credential.Proxy             import Proxy


class VanillaCondorPlugin(CondorPlugin):
    """
    _VanillaCondorPlugin_

    Minor variation on standard glide-in based plugin to allow
    us to submit vanilla jobs.
    """

    def customizePerJob(self, job):
        """
        JDL additions to common JDL part for vanilla condor implementation.
        These are the Vanilla condor specific bits
        """
        jdl = []
        # Check for multicore. Not sure this is valid JDL, but it's what we were doing.
        if job.get('taskType', None) in self.multiTasks:
            jdl.append('+RequiresWholeMachine?' 'TRUE')
        return jdl

    def customizeCommon(self, jobList):
        """
        JDL additions for this job for vanilla condor implementation.
        These are the Vanilla condor specific bits
        """
        jdl = []
        return jdl
