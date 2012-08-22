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
    def addAnalysisFile(self, infile):
        """
        Enqueues an analysis file to the DQMUpload step
        infile must be a PFN (DQM file)
        """
        target = self.data.analysisFiles.section_(
                    "file%i" % self.data.filecount)
        target.input  = infile
        self.data.analysisFiles.filecount += 1 
    
    def disableRetries(self):
        """
        handy for testing, without the 10 minute retry loop
        """
        self.data.stageOut.retryCount = 1
        self.data.stageOut.retryDelay = 0

    def disableUpload(self):
        """
        In case we don't want to upload file to any server
        """
        self.data.upload.active = False

    def disableStageOut(self):
        """
        In case we don't want to stage out any file
        """
        self.data.stageOut.active = False

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
        step.section_("analysisFiles")
        step.analysisFiles.filecount = 0
        step.section_("upload")
        step.upload.active = True
        step.upload.URL = 'https://cmsweb.cern.ch/dqm/dev'
        step.upload.proxy = None
        step.section_("stageOut")
        step.stageOut.active = False
        step.stageOut.retryCount = 3
        step.stageOut.retryDelay = 300
        #step.stageOut.waitTime = 1200

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return DQMUploadStepHelper(step)



