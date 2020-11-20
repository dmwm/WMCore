#!/usr/bin/env python
"""
_BasePlugin_

Base class for BossAir plugins
"""

from WMCore.WMException import WMException
from WMCore.WMRuntime.Tools.Scram import ARCH_TO_OS



class BossAirPluginException(WMException):
    """

    Generalized exception class for BossAir Plugins


    """




class BasePlugin(object):
    """
    Base class for BossAir Plugins

    Does nothing useful, just to be overridden later
    """


    # the common states which needs to be mapped from each plugIn states
    globalState = ['Pending', 'Running', 'Complete', 'Error']

    @staticmethod
    def verifyState(stateMap):
        """
        Verify state of map values
        """
        for state in stateMap.values():
            if state not in BasePlugin.globalState:
                raise BossAirPluginException("not valid state %s" % state)
        return stateMap

    @staticmethod
    def stateMap():
        """
        Empty state map to allow instantiation of base class
        """
        stateDict = {}

        return stateDict

    def __init__(self, config):

        self.config = config

        # NOTE: Don't overwrite this.
        # However stateMap should be implemented in child class.
        self.states = self.stateMap().keys()



    def submit(self, jobs, info=None):
        """
        _submit_

        Submits jobs
        """

        pass


    def track(self, jobs):
        """
        _track_

        Tracks jobs
        Returns three lists:
        1) the running jobs
        2) the jobs that need to be updated in the DB
        3) the complete jobs
        """

        return jobs, jobs, []


    def complete(self, jobs):
        """
        _complete_

        Run any complete code
        """


        pass


    def kill(self, jobs, raiseEx):
        """
        _kill_

        Kill any and all jobs
        """

        pass

    def updateJobInformation(self, workflow, task, **kwargs):
        """
        _updateJobInformation_

        Update information on pending jobs
        where supported, the values are updated
        are passed through the kwargs
        """
        pass

    def updateSiteInformation(self, jobs, siteName, excludeSite):
        """
        _updateSiteInformation_

        Update Site Information
        """
        pass

    @staticmethod
    def scramArchtoRequiredOS(scramArch=None):
        """

        Args:
            scramArch: string or list of scramArches that are acceptable for the job

        Returns:
            string to be matched for OS requirements for job
        """
        requiredOSes = set()
        if scramArch is None:
            requiredOSes.add('any')
        elif isinstance(scramArch, basestring):
            for arch, validOSes in ARCH_TO_OS.iteritems():
                if arch in scramArch:
                    requiredOSes.update(validOSes)
        elif isinstance(scramArch, list):
            for validArch in scramArch:
                for arch, validOSes in ARCH_TO_OS.iteritems():
                    if arch in validArch:
                        requiredOSes.update(validOSes)
        else:
            requiredOSes.add('any')

        return ','.join(sorted(requiredOSes))
