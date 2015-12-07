#!/usr/bin/env python
"""
_DQMUpload_

Template for a DQMUpload Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName


class DQMUploadStepHelper(CoreHelper):
    """
    _DQMUploadStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for DQMUpload tasks

    """
    def setProxyFile(self, proxy):
        """
        Sets the name of the proxy file in the input sandbox
        """
        self.data.upload.proxy = proxy

    def setServerURL(self, url):
        """
        Sets the URL of the DQM GUI server.
        """
        self.data.upload.URL = url


class DQMUpload(Template):
    """
    _DQMUpload_

    Tools for creating a template DQMUpload Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "DQMUpload"
        step.section_("upload")
        step.upload.URL = 'https://cmsweb.cern.ch/dqm/dev'
        step.upload.proxy = None

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return DQMUploadStepHelper(step)
