#!/usr/bin/env python
"""
_CMSSW_

Template for a CMSSW Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName

class CMSSWStepHelper(WMStepHelper):
    """
    _CMSSWStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for CMSSW tasks

    """
    def addOutputModule(self, moduleName, **details):
        """
        _addOutputModule_

        Add in an output module settings, all default to None unless
        the value is provided in details

        """





class CMSSW(Template):
    """
    _CMSSW_

    Tools for creating a template CMSSW Step

    """

    def install(self, step):
        """
        _install_

        Add the set of default fields to the step required for running
        a cmssw job

        """
        stepname = nodeName(step)
        step.stepType = "CMSSW"
        step.application.section_("setup")
        step.application.setup.scramProject = "CMSSW"
        step.application.setup.cmsswVersion = None
        step.application.setup.scramArch = None
        step.application.setup.softwareEnvironment = None

        step.application.section_("command")
        step.application.command.executable = "cmsRun"
        step.application.command.arguments = ""
        step.output.jobReport = "FrameworkJobReport.xml"
        step.output.stdout = "%s-stdout.log" % stepname
        step.output.stderr = "%s-stderr.log" % stepname
        step.output.section_("modules")


        print step


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return CMSSWStepHelper(step)

