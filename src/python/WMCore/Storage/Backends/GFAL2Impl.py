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
        # If we want to execute commands in clean shell, we can`t separate them with ';'.
        # Next commands after separation are executed without env -i and this leads us with 
        # mixed environment with COMP and system python.
        # GFAL2 is not build under COMP environment and it had failures with mixed environment.
        self.setups = "env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c '%s'"
        self.removeCommand = self.setups % '. $JOBSTARTDIR/startup_environment.sh; printenv; date; gfal-rm -vvv -t 600 %s '
        self.copyCommand = self.setups % '. $JOBSTARTDIR/startup_environment.sh; printenv; date; gfal-copy -vvv -t 2400 -T 2400 -p %(checksum)s %(options)s %(source)s %(destination)s'

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
        _createRemoveFileCommand_
        handle file remove using gfal-rm

        gfal-rm options used:
          -vvv most verbose mode
          -t   global timeout for the execution of the command.
               Command is interrupted if time expires before it finishes
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
        Build a gfal-copy command

        gfal-copy options used:
          -vvv most verbose mode
          -t   maximum time for the operation to terminate
          -T   global timeout for the transfer operation
          -p   if the destination directory does not exist, create it
          -K   checksum algorithm to use, or algorithm:value
        """
        result = "#!/bin/bash\n"

        useChecksum = (checksums != None and 'adler32' in checksums and not self.stageIn)

        copyCommandDict = {'checksum': '', 'options': '', 'source': '', 'destination': ''}
        if useChecksum:
            copyCommandDict['checksum'] = "-K adler32"
        if options != None:
            copyCommandDict['options'] = options
        copyCommandDict['source'] = sourcePFN
        copyCommandDict['destination'] = targetPFN

        copyCommand = self.copyCommand % copyCommandDict
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
