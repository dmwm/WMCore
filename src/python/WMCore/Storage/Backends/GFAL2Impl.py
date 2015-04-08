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
            return "env -i gfal-rm -vvv %s" % pfn
        elif os.path.isfile(pfn):
            return "/bin/rm -f %s" % os.path.abspath(pfn)
        else:
            return "env -i gfal-rm -vvv %s" % pfn


    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        _createStageOutCommand_
        Build an gfal-copy command
        """
        result = "#!/bin/sh\n"

        useChecksum = (checksums != None and 'adler32' in checksums and not self.stageIn)

        copyCommand = "env -i gfal-copy -t 2400 -T 2400 -p -vvv "
        if useChecksum:
            copyCommand += "-K adler32 "
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
            command = "env -i gfal-rm -vvv %s" % pfnToRemove
        else:
            command = "env -i gfal-rm -vvv %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("gfal2", GFAL2Impl)
