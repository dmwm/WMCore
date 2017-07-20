#!/usr/bin/env python
"""
_GFAL2Impl_
Implementation of StageOutImpl interface for gfal-copy
"""
import argparse
import os

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

_CheckExitCodeOption = True


class GFAL2Impl(StageOutImpl):
    """
    _GFAL2Impl_
    Implement interface for GFAL2 commands (gfal-copy, gfal-rm)
    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        # If we want to execute commands in clean shell, we can`t separate them with ';'.
        # Next commands after separation are executed without env -i and this leads us with
        # mixed environment with COMP and system python.
        # GFAL2 is not build under COMP environment and it had failures with mixed environment.
        self.setups = "env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c '%s'"
        self.removeCommand = self.setups % '. $JOBSTARTDIR/startup_environment.sh; date; gfal-rm -t 600 %s '
        self.copyCommand = self.setups % '. $JOBSTARTDIR/startup_environment.sh; date; gfal-copy -t 2400 -T 2400 -p %(checksum)s %(options)s %(source)s %(destination)s'

    def createFinalPFN(self, pfn):
        """
        _createFinalPFN_
        GFAL2 requires file:/// for any direction transfers
        """
        if pfn.startswith('file:'):
            return pfn
        elif os.path.isfile(pfn):
            return "file://%s" % os.path.abspath(pfn)
        elif pfn.startswith('/'):
            return "file://%s" % os.path.abspath(pfn)
        return pfn

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_
        GFAL2 uses file:/// urls
        """
        return self.createFinalPFN(pfn)

    def createTargetName(self, protocol, pfn):
        """
        _createTargetName_
        GFAL2 uses file:// urls
        """
        return self.createFinalPFN(pfn)

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
          -t   global timeout for the execution of the command.
               Command is interrupted if time expires before it finishes
        """
        if os.path.isfile(pfn):
            return "/bin/rm -f %s" % os.path.abspath(pfn)
        else:
            return self.removeCommand % self.createFinalPFN(pfn)

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        _createStageOutCommand_
        Build a gfal-copy command

        gfal-copy options used:
          -t   maximum time for the operation to terminate
          -T   global timeout for the transfer operation
          -p   if the destination directory does not exist, create it
          -K   checksum algorithm to use, or algorithm:value
        """
        result = "#!/bin/bash\n"

        copyCommandDict = {'checksum': '', 'options': '', 'source': '', 'destination': ''}

        useChecksum = (checksums is not None and 'adler32' in checksums and not self.stageIn)

        if not options:
            options = ''

        parser = argparse.ArgumentParser()
        parser.add_argument('--nochecksum', action='store_true')
        args, unknown = parser.parse_known_args(options.split())

        if not args.nochecksum:
            if useChecksum:
                checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)
                copyCommandDict['checksum'] = "-K adler32:%s" % checksums['adler32']
            else:
                copyCommandDict['checksum'] = "-K adler32"

        copyCommandDict['options'] = ' '.join(unknown)

        copyCommandDict['source'] = self.createFinalPFN(sourcePFN)
        copyCommandDict['destination'] = self.createFinalPFN(targetPFN)

        copyCommand = self.copyCommand % copyCommandDict
        result += copyCommand

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=$?
            echo "gfal-copy exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
               echo "ERROR: gfal-copy exited with $EXIT_STATUS"
               echo "Cleaning up failed file:"
               %s
            fi
            exit $EXIT_STATUS
            """ % self.createRemoveFileCommand(targetPFN)

        return result

    def removeFile(self, pfnToRemove):
        """
        _removeFile_
        CleanUp pfn provided
        """
        if os.path.isfile(pfnToRemove):
            command = "/bin/rm -f %s" % os.path.abspath(pfnToRemove)
        else:
            command = self.removeCommand % self.createFinalPFN(pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("gfal2", GFAL2Impl)
