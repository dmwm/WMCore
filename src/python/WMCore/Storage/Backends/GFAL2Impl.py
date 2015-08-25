#!/usr/bin/env python
"""
_GFAL2Impl_
Implementation of StageOutImpl interface for gfal-copy
"""
import os
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True


class GFAL2Impl(StageOutImpl):
    """
    _GFAL2Impl_
    Implement interface for GFAL2 commands (gfal-copy, gfal-rm)
    """
    run = staticmethod(runCommand)

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.removeCommand = "env -i X509_USER_PROXY=$X509_USER_PROXY gfal-rm -vvv %s"
        self.copyCommand = "env -i X509_USER_PROXY=$X509_USER_PROXY gfal-copy -t 2400 -T 2400 -p -vvv"

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_
        GFAL2 uses file:/// urls
        """
        if pfn.startswith('file:'):
            return pfn
        elif os.path.isfile(pfn):
            return "file://%s" % os.path.abspath(pfn)
        else:
            return pfn


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_
        gfal-copy handles this with option -p and
        it is not needed to create directory
        """
        return


    def createRemoveFileCommand(self, pfn):
        """
        handle file remove using gfal-rm
        """
        if pfn.startswith("file:"):
            return self.removeCommand % pfn
        elif os.path.isfile(pfn):
            return "/bin/rm -f %s" % os.path.abspath(pfn)
        else:
            return self.removeCommand % pfn


    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        _createStageOutCommand_
        Build an gfal-copy command
        """
        result = "#!/bin/sh\n"

        useChecksum = (checksums != None and 'adler32' in checksums and not self.stageIn)

        copyCommand = self.copyCommand
        if useChecksum:
            copyCommand += " -K ADLER32 "
        if options != None:
            copyCommand += " %s " % options
        copyCommand += " %s " % sourcePFN
        copyCommand += " %s \n" % targetPFN

        result += copyCommand

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=$?
            echo "gfal-copy exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero gfal-copy Exit status!!!"
               echo "Cleaning up failed file:"
                %s
               exit 60311
            fi
            exit 0
            """ % self.createRemoveFileCommand(targetPFN)

        return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_
        CleanUp pfn provided
        """
        command = ""
        if os.path.isfile(pfnToRemove):
            command = "/bin/rm -f %s" % os.path.abspath(pfnToRemove)
        if pfnToRemove.startswith("file:"):
            command = self.removeCommand % pfnToRemove
        else:
            command = self.removeCommand % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("gfal2", GFAL2Impl)
