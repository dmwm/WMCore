#!/usr/bin/env python
"""
_GFAL2Impl_
Implementation of StageOutImpl interface for gfal-copy
"""
import argparse
import os

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


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

        self.setAuthX509 = "X509_USER_PROXY=$X509_USER_PROXY"
        self.setAuthToken = "BEARER_TOKEN_FILE=$BEARER_TOKEN_FILE BEARER_TOKEN=$(cat $BEARER_TOKEN_FILE)"
        self.unsetX509 = "unset X509_USER_PROXY;"
        self.unsetToken = "unset BEARER_TOKEN;"

        # These commands are parameterized according to:
        # 1. authentication method (set_auth)
        # 2. forced authentication method (unset_auth)
        # 3. finally, debug mode or not (dry_run)
        self.setups = "env -i {{set_auth}} JOBSTARTDIR=$JOBSTARTDIR bash -c '{}'"
        self.copyOpts = '-t 2400 -T 2400 -p -v --abort-on-failure {checksum} {options} {source} {destination}'
        self.copyCommand = self.setups.format('. $JOBSTARTDIR/startup_environment.sh; {unset_auth} date; {dry_run} gfal-copy ' + self.copyOpts)
        self.removeCommand = self.setups.format('. $JOBSTARTDIR/startup_environment.sh; {unset_auth} date; {dry_run} gfal-rm -t 600 {}')

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

    def createRemoveFileCommand(self, pfn, authMethod=None, forceMethod=False, dryRun=False):
        """
        handle file remove using gfal-rm

        gfal-rm options used:
          -t   global timeout for the execution of the command.
               Command is interrupted if time expires before it finishes
        :pfn: str, destination PFN
        :authMethod: string with the authentication method to use (either 'X509' or 'TOKEN')
        :forceMethod: bool to isolate and force a given authentication method
        :dryRun: bool, dry run mode (to enable debug mode)
        """
        if authMethod is None:
            set_auth = self.setAuthToken + " " + self.setAuthX509
            unset_auth = ""
        elif authMethod == 'X509':
            set_auth = self.setAuthX509
            unset_auth = self.unsetToken if forceMethod else ""
        elif authMethod == 'TOKEN':
            set_auth = self.setAuthToken
            unset_auth = self.unsetToken if forceMethod else ""
        
        dryRun = 'echo ' if dryRun else ''

        if os.path.isfile(pfn):
            return "{} /bin/rm -f {}".format(dryRun, os.path.abspath(pfn))
        else:
            return self.removeCommand.format(self.createFinalPFN(pfn), set_auth=set_auth, unset_auth=unset_auth, dry_run=dryRun)

    def buildCopyCommandDict(self, sourcePFN, targetPFN, options=None, checksums=None,
                             authMethod='X509', forceMethod=False, dryRun=False):
        """
        Build the gfal-copy command for stageOut

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        :authMethod: string with the authentication method to use (either 'X509' or 'TOKEN')
        :forceMethod: bool to isolate and force a given authentication method
        :dryRun: bool, dry run mode (to enable debug mode)
        :returns: a dictionary with specific parameters to be formatted in the commands
        """

        copyCommandDict = {'checksum': '', 'options': '', 'source': '', 'destination': '',
                           'set_auth': '', 'unset_auth': '', 'dry_run': ''}

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

        if authMethod is None:
            copyCommandDict['set_auth'] = ""
            copyCommandDict['unset_auth'] = ""
        elif authMethod.upper() == 'X509':
            copyCommandDict['set_auth'] = self.setAuthX509
            copyCommandDict['unset_auth'] = self.unsetToken if forceMethod else ""
        elif authMethod.upper() == 'TOKEN':
            copyCommandDict['set_auth'] = self.setAuthToken
            copyCommandDict['unset_auth'] = self.unsetToken if forceMethod else ""

        copyCommandDict['dry_run'] = 'echo ' if dryRun else ''

        return copyCommandDict

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, 
                              authMethod=None, forceMethod=False):
        """
        Create gfal-cp command for stageOut

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        :authMethod: string with the authentication method to use (either 'X509' or 'TOKEN')
        :forceMethod: bool to isolate and force a given authentication method
        returns: a string with the full stage out script
        """
        copyCommandDict = self.buildCopyCommandDict(sourcePFN, targetPFN, options, checksums,
                                                    authMethod, forceMethod)
        copyCommand = self.copyCommand.format_map(copyCommandDict)
        if authMethod is None:
            # add more verbosity for reporting which authentication method GFAL uses when it is not specified
            copyCommand = copyCommand.replace('-p -v', '-p -vvv')
        
        result = "#!/bin/bash\n" + copyCommand

        # add check for exit code
        result += """
        EXIT_STATUS=$?
        echo "gfal-copy exit status: $EXIT_STATUS"
        if [[ $EXIT_STATUS != 0 ]]; then
            echo "ERROR: gfal-copy exited with $EXIT_STATUS"
            echo "Cleaning up failed file:"
            {remove_command}
        fi
        exit $EXIT_STATUS
        """.format(remove_command=self.createRemoveFileCommand(targetPFN, authMethod=authMethod, forceMethod=forceMethod))

        return result

    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None,
                               authMethod=None, forceMethod=False):
        """
        Debug a failed gfal-cp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """

        copyCommandDict = self.buildCopyCommandDict(sourcePFN, targetPFN, options, checksums,
                                                    authMethod, forceMethod, dryRun=True)

        copyCommand = self.copyCommand.format_map(copyCommandDict)
        if authMethod is None:
            # add more verbosity for reporting which authentication method GFAL uses when it is not specified
            copyCommand = copyCommand.replace('-p -v', '-p -vvv')

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
        echo "Information for credentials in the environment"
        echo "Bearer token content: $BEARER_TOKEN"
        echo "Bearer token file: $BEARER_TOKEN_FILE"
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
        command = self.createRemoveFileCommand(pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("gfal2", GFAL2Impl)