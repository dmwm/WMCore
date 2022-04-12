#!/usr/bin/env python
"""
_BasePlugin_

Base class for BossAir plugins
"""

from builtins import object, str, bytes
from future.utils import viewitems, viewvalues

from Utils.Utilities import decodeBytesToUnicode
from WMCore.WMException import WMException
from WMCore.WMRuntime.Tools.Scram import ARCH_TO_OS, SCRAM_TO_ARCH



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
        for state in viewvalues(stateMap):
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
        self.states = list(self.stateMap())



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

    def updateJobInformation(self, workflow, **kwargs):
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
        if not scramArch:
            requiredOSes.add('any')
        elif isinstance(scramArch, (str, bytes)):
            for arch, validOSes in viewitems(ARCH_TO_OS):
                if arch in scramArch:
                    requiredOSes.update(validOSes)
        elif isinstance(scramArch, list):
            for validArch in scramArch:
                for arch, validOSes in viewitems(ARCH_TO_OS):
                    if arch in validArch:
                        requiredOSes.update(validOSes)
        else:
            requiredOSes.add('any')

        return ','.join(sorted(requiredOSes))

    @staticmethod
    def scramArchtoRequiredArch(scramArch=None):
        """
        Converts a given ScramArch to a unique target CPU architecture.
        Note that an architecture precedence is enforced in case there are
        multiple matches.
        In case no scramArch is defined, leave the architecture undefined.
        :param scramArch: can be either a string or a list of ScramArchs
        :return: a string with the matched architecture
        """
        defaultArch = "X86_64"
        requiredArchs = set()
        if scramArch is None:
            return None
        elif isinstance(scramArch, (str, bytes)):
            scramArch = [scramArch]

        for item in scramArch:
            item = decodeBytesToUnicode(item)
            arch = item.split("_")[1]
            if arch not in SCRAM_TO_ARCH:
                msg = "Job configured to a ScramArch: '{}' not supported in BossAir".format(item)
                raise BossAirPluginException(msg)
            requiredArchs.add(SCRAM_TO_ARCH.get(arch))

        # now we have the final list of architectures, return only 1 of them
        if len(requiredArchs) == 1:
            return requiredArchs.pop()
        elif "X86_64" in requiredArchs:
            return "X86_64"
        elif "ppc64le" in requiredArchs:
            return "ppc64le"
        elif "aarch64" in requiredArchs:
            return "aarch64"
        else:  # should never get here!
            return defaultArch
