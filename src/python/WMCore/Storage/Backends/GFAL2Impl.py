#!/usr/bin/env python
"""
_GFAL2Impl_
Implementation of StageOutImpl interface for gfal-copy
"""
import argparse
import os
import logging

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Algorithms.SubprocessAlgos import SubprocessAlgoException, runCommand

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
        self.copyOpts = '-t 2400 -T 2400 -p -v --abort-on-failure %(checksum)s %(options)s %(source)s %(destination)s'
        self.copyCommand = self.setups % ('. $JOBSTARTDIR/startup_environment.sh; date; gfal-copy ' + self.copyOpts)

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
                checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)
                copyCommandDict['checksum'] = "-K adler32:%s" % checksums['adler32']
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
        copyCommand = self.copyCommand % copyCommandDict
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
            """.format(
                remove_command=self.createRemoveFileCommand(targetPFN)
            )
        
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
        copyCommand = self.copyCommand % copyCommandDict

        try:
            stdout, stderr, returnCode = runCommand("voms-proxy-info -file $X509_USER_PROXY", timeout=10)
            if returnCode == 0:
                logging.info("Success! voms-proxy-info output:\n%s", stdout)       
            else:
                logging.error("Failure! voms-proxy-info failed with return code: %s", returnCode)
                logging.error("Error output: %s", stderr)
        except SubprocessAlgoException as e:
            logging.error("An exception occurred while running voms-proxy-info:")
            logging.error(e)

        logging.error("Actual gfal-cp command which failed: %s", copyCommand)

        result = "#!/bin/bash\n"
        result += """
        echo "gfal-cp command which failed: {copy_command}"
        echo "gfal-cp location:"
        which gfal-cp
        echo "gfal-cp location: $(which gfal-cp)".
        echo "Source PFN: {source}"
        echo "Target PFN: {destination}"
        echo "Proxy and JOBSTARTDIR: {setup_info}"
        echo "Hostname:   $(hostname -f)"
        echo "OS:  $(uname -n -r -s)"
        """.format(
            copy_command=copyCommand,
            source=copyCommandDict['source'],
            destination=copyCommandDict['destination'],
            setup_info=self.setups
        )
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
