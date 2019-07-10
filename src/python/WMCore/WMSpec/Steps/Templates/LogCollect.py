#!/usr/bin/env python
"""
_LogCollect_

Template for a LogCollect Step

Mostly borrowed from StageOut since they share a similar function

"""




from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper


class LogCollectStepHelper(CoreHelper):
    """
    _StageOutStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for StageOut tasks

    This is very similar to StageOut since they have essentially the same function

    """

    def disableRetries(self):
        """
            handy for testing, without the 10 minute retry loop
        """
        self.data.retryCount = 1
        self.data.retryDelay = 0

    def addOutputDestination(self, lfn):
        """
        Adds an out location to put a tarball of all logs
        """

    def cmsswSetup(self, cmsswVersion, **options):
        """
        _cmsswSetup_
        Provide setup details for CMSSW.
        cmsswVersion - required - version of CMSSW to use
        Optional:
        scramCommand - defaults to scramv1
        scramProject - defaults to CMSSW
        scramArch    - optional scram architecture, defaults to None
        buildArch    - optional scram build architecture, defaults to None
        softwareEnvironment - setup command to bootstrap scram,defaults to None
        """
        self.data.application.setup.cmsswVersion = cmsswVersion
        for k, v in options.items():
            setattr(self.data.application.setup, k, v)
        return

    def getScramArch(self):
        """
        _getScramArch_

        Retrieve the scram architecture used for this step.
        """
        return self.data.application.setup.scramArch

    def getCMSSWVersion(self):
        """
        _getCMSSWVersion_

        Retrieve the version of the framework used for this step.
        """
        return self.data.application.setup.cmsswVersion

class LogCollect(Template):
    """
    _LogCollect_

    Tools for creating a template LogCollect Step

    """

    def install(self, step):
        step.stepType = "LogCollect"
        step.section_("logs")
        step.logcount = 0
        step.retryCount = 3
        step.retryDelay = 300
        step.application.section_("setup")
        step.application.setup.scramCommand = "scramv1"
        step.application.setup.scramProject = "CMSSW"
        step.application.setup.cmsswVersion = None
        step.application.setup.scramArch = None
        step.application.setup.buildArch = None
        step.application.setup.softwareEnvironment = None

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return LogCollectStepHelper(step)
