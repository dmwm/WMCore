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
        self.setups = "env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c '{}'"
        self.removeCommand = self.setups.format('. $JOBSTARTDIR/startup_environment.sh; date; gfal-rm -t 600 {}')
        self.copyOpts = '-t 2400 -T 2400 -p -v --abort-on-failure {checksum} {options} {source} {destination}'
        self.copyCommand = self.setups.format('. $JOBSTARTDIR/startup_environment.sh; date; gfal-copy ' + self.copyOpts)

    def createFinalPFN(self, pfn):
        """
        _createFinalPFN_
        GFAL2 requires file:/// for any direction transfers
        """
        if pfn.startswith('file:'):
            return pfn
        elif os.path.isfile(pfn):
            return "file://{}".format(os.path.abspath(pfn))
        elif pfn.startswith('/'):
            return "file://{}".format(os.path.abspath(pfn))
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
            return "/bin/rm -f {}".format(os.path.abspath(pfn))
        else:
            return self.removeCommand.format(self.createFinalPFN(pfn))


    def buildCopyCommandDict(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        Build the gfal-cp command for stageOut

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """

        copyCommandDict = {'checksum': '', 'options': '', 'source': '', 'destination': ''}

        useChecksum = (checksums is not None and 'adler32' in checksums and not self.stageIn)

        if not options:
            options = ''

        parser = argparse.ArgumentParser()
        parser.add_argument('--nochecksum', action='store_true')
        args, unknown = parser.parse_known_args(options.split())

        if not args.nochecksum:
            if useChecksum:
                checksums['adler32'] = "{:08x}".format(int(checksums['adler32'], 16))
                copyCommandDict['checksum'] = "-K adler32:{}".format(checksums['adler32'])
            else:
                copyCommandDict['checksum'] = "-K adler32"

        copyCommandDict['options'] = ' '.join(unknown)
        copyCommandDict['source'] = self.createFinalPFN(sourcePFN)
        copyCommandDict['destination'] = self.createFinalPFN(targetPFN)

        return copyCommandDict

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        Create gfal-cp command for stageOut

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """

        copyCommandDict = self.buildCopyCommandDict(sourcePFN, targetPFN, options, checksums)
        copyCommand = self.copyCommand.format_map(copyCommandDict)
        result = "#!/bin/bash\n" + copyCommand

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=$?
            echo "gfal-copy exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
                echo "ERROR: gfal-copy exited with $EXIT_STATUS"
                echo "Cleaning up failed file:"
                {remove_command}
            fi
            exit $EXIT_STATUS
            """.format(remove_command=self.createRemoveFileCommand(targetPFN))

        return result

    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        Debug a failed gfal-cp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """

        copyCommandDict = self.buildCopyCommandDict(sourcePFN, targetPFN, options, checksums)
        copyCommand = self.copyCommand.format_map(copyCommandDict)

        result = "#!/bin/bash\n"
        result += """
        echo
        echo
        echo "-----------------------------------------------------------"
        echo "==========================================================="
        echo
        echo "Debugging information on failing gfal-copy command"
        echo
        echo "Current date and time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "gfal-copy command which failed: {copy_command}"
        echo "Hostname:   $(hostname -f)"
        echo "OS:  $(uname -r -s)"
        echo
        echo "GFAL environment variables:"
        env | grep ^GFAL
        echo
        echo "PYTHON environment variables:"
        env | grep ^PYTHON
        echo
        echo "LD_* environment variables:"
        env | grep ^LD_
        echo
        echo "gfal-copy location: $(which gfal-copy)"
        echo "Source PFN: {source}"
        echo "Target PFN: {destination}"
        echo
        echo "VOMS proxy info:"
        voms-proxy-info -all
        echo "==========================================================="
        echo "-----------------------------------------------------------"
        echo
        """.format(copy_command=copyCommand, source=copyCommandDict['source'], destination=copyCommandDict['destination'])
        return result

    def removeFile(self, pfnToRemove):
        """
        _removeFile_
        CleanUp pfn provided
        """
        if os.path.isfile(pfnToRemove):
            command = "/bin/rm -f {}".format(os.path.abspath(pfnToRemove))
        else:
            command = self.removeCommand.format(self.createFinalPFN(pfnToRemove))
        self.executeCommand(command)


registerStageOutImpl("gfal2", GFAL2Impl)
