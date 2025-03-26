#!/usr/bin/env python
"""
_TestImpl_

Couple of test implementations for unittests

"""
from __future__ import print_function

import os
import os.path

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutFailure

class WinImpl(StageOutImpl):
    """
    _WinImpl_

    Test plugin that always returns success

    """
    def createSourceName(self, protocol, pfn):
        return "WIN!!!"


    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=False, forceMethod=False):
        print("WinImpl.createStageOutCommand(%s, %s, %s, %s)" % (sourcePFN, targetPFN, options, checksums))
        return "WIN!!!"


    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed unittest cp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommand = "WinImpl.createStageOutCommand(%s, %s, %s, %s)" % (sourcePFN, targetPFN, options, checksums)

        result = "#!/bin/bash\n"
        result += """
        echo
        echo
        echo "-----------------------------------------------------------"
        echo "==========================================================="
        echo
        echo "Debugging information on failing copy command"
        echo
        echo "Current date and time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "copy command which failed: {copy_command}"
        echo "Hostname:   $(hostname -f)"
        echo "OS:  $(uname -r -s)"
        echo
        echo "PYTHON environment variables:"
        env | grep ^PYTHON
        echo
        echo "LD_* environment variables:"
        env | grep ^LD_
        echo
        echo "Source PFN: {source}"
        echo "Target PFN: {destination}"
        echo
        echo "VOMS proxy info:"
        voms-proxy-info -all
        echo "==========================================================="
        echo "-----------------------------------------------------------"
        echo
        """.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)
        return result


    def removeFile(self, pfnToRemove):
        print("WinImpl.removeFile(%s)" % pfnToRemove)
        return "WIN!!!"


    def executeCommand(self, command):
        return 0



class FailImpl(StageOutImpl):
    """
    _FailImpl_

    Test plugin that always results in a StageOutFailure

    """

    def createSourceName(self, protocol, pfn):
        return "FAIL!!!"


    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        print("FailImpl.createStageOutCommand(%s, %s, %s, %s)" % (sourcePFN, targetPFN, options, checksums))
        return "FAIL!!!"


    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed unittest cp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommand = "FailImpl.createStageOutCommand(%s, %s, %s, %s)" % (sourcePFN, targetPFN, options, checksums)

        result = self.debuggingTemplate.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)
        return result

    def removeFile(self, pfnToRemove):
        print("FailImpl.removeFile(%s)" % pfnToRemove)
        return "FAIL!!!"


    def executeCommand(self, command):
        msg = "FailImpl returns FAIL!!!"
        raise StageOutFailure( msg)


class LocalCopyImpl(StageOutImpl):
    """
    _LocalCopyImp_

    Test plugin that copies to a local directory

    """

    def createOutputDirectory(self, targetPFN):
        """
        I guess this masks a directory?

        """

        dirName = os.path.dirname(targetPFN)

        if not os.path.isdir(dirName):
            os.makedirs(dirName)

        return


    def createSourceName(self, protocol, pfn):
        """
        This should return the same PFN
        """
        return pfn


    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        command = "cp %s %s" %(sourcePFN, sourcePFN+'2')
        return command

    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed unittest cp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommand = "cp %s %s" %(sourcePFN, sourcePFN+'2')

        result = self.debuggingTemplate.format(copy_command=copyCommand, source=sourcePFN, destination=sourcePFN+'2')
        return result

    def removeFile(self, pfnToRemove):
        command = "rm  %s" %pfnToRemove
        self.executeCommand(command)
        return "WIN!!!"


registerStageOutImpl("test-win", WinImpl)
registerStageOutImpl("test-fail", FailImpl)
registerStageOutImpl("test-copy", LocalCopyImpl)
