#!/usr/bin/env python
"""
_XRDCPImpl_

Implementation of StageOutImpl interface for xrdcp

Generic, will/should work with any site.

"""
from __future__ import print_function

import argparse
import logging
import os

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

class XRDCPImpl(StageOutImpl):
    """
    _XRDCPImpl_

    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300
        self.xrdfsCmd = "xrdfs"
        self.setAuthX509 = "env X509_USER_PROXY=$X509_USER_PROXY "
        self.setAuthToken = "env BEARER_TOKEN_FILE=$BEARER_TOKEN_FILE BEARER_TOKEN=$(cat $BEARER_TOKEN_FILE) "
        self.unsetX509 = "X509_USER_PROXY= "
        self.unsetToken = "BEARER_TOKEN_FILE= BEARER_TOKEN= "
        self.debuggingTemplate = "#!/bin/bash\n"
        self.debuggingTemplate += """
        echo
        echo
        echo "-----------------------------------------------------------"
        echo "==========================================================="
        echo
        echo "Debugging information on failing xrdcp/xrdfs command"
        echo
        echo "Current date and time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "XRootD command which failed: {copy_command}"
        echo "Hostname:   $(hostname -f)"
        echo "OS:  $(uname -r -s)"
        echo
        echo "XRD environment variables (if any):"
        env | grep ^XRD_
        echo
        echo "PYTHON environment variables:"
        env | grep ^PYTHON
        echo
        echo "LD_* environment variables:"
        env | grep ^LD_
        echo
        echo "xrdcp location: $(which xrdcp)"
        echo "xrdfs location: $(which xrdfs)"
        echo "Source PFN: {source}"
        echo "Target PFN: {destination}"
        echo
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
        """
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        """
        return pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        not needed since xrdcp does it automatically
        """
        return

    def checkXRDUtilsExist(self):
        """
        Verifies whether xrdcp and xrdfs utils exist in the job path.

        :return: True if both exist, otherwise False
        """
        foundXrdcp, foundXrdfs = False, False
        for path in os.environ["PATH"].split(os.pathsep):
            if os.access(os.path.join(path, "xrdcp"), os.X_OK):
                foundXrdcp = True
            if os.access(os.path.join(path, "xrdfs"), os.X_OK):
                foundXrdfs = True

        return foundXrdcp & foundXrdfs

    def _getAuthEnv(self, authMethod=None, forceMethod=False):
        """
        Get environment variables for stageout command based on the selected authentication method.
        :authMethod: str, the authentication method to be used ("X509", "TOKEN", or None)
        :forceMethod: bool, cleans non-chosen auth methods from environment.
        :return: str
        """
        authEnv = ""
        if authMethod is None:
            return authEnv
        elif authMethod.upper() == 'X509':
            authEnv = self.setAuthX509
            if forceMethod:
                authEnv += self.unsetToken
        elif authMethod.upper() == 'TOKEN':
            authEnv = self.setAuthToken
            if forceMethod:
                authEnv += self.unsetX509
        else:
            logging.warning("Warning! Running without either a X509 certificate or a token specified!")

        return authEnv

    def getFormattedCopyCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False, dryRun=False):
        """
        Construct xrdcp/xrdfs stageout command.
        If adler32 checksum is provided, use it for the transfer
        xrdcp options used:
          --force : re-creates a file if it's already present
          --nopbar : does not display the progress bar

        :sourcePFN: str, the source PFN.
        :targetPFN: str, the target PFN.
        :options: str, additional options for gfal-copy.
        :checksums: dict, checksum values.
        :authMethod: str, preferred authentication method.
        :forceMethod: bool, whether to force the preferred authentication method.
        :dryRun: bool, dry run mode (to enable debug mode)
        """
        if not options:
            options = ''

        parser = argparse.ArgumentParser()
        parser.add_argument('--wma-cerncastor', action='store_true')
        parser.add_argument('--wma-disablewriterecovery', action='store_true')
        parser.add_argument('--wma-preload')
        args, unknown = parser.parse_known_args(options.split())

        # strip out WMAgent specific options
        unknown = [option for option in unknown if not option.startswith('--wma-')]
        copyCommandOptions = ' '.join(unknown)

        copyCommand = ""

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            if args.wma_cerncastor:
                targetPFN += "?svcClass=t0cms"

        useChecksum = (checksums is not None and 'adler32' in checksums and not self.stageIn)

        # If not, check whether OSG cvmfs repo is available.
        # This likely only works on RHEL7 x86-64 (and compatible) OS, but this
        # represents most of our resources and it's a fallback mechanism anyways
        if not self.checkXRDUtilsExist():
            logging.warning("Failed to find XRootD in the path. Trying fallback to OSG CVMFS...")
            initFile = "/cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el7-x86_64/setup.sh"
            if os.path.isfile(initFile):
                copyCommand += "source %s\n" % initFile

        if args.wma_disablewriterecovery:
            copyCommand += "env XRD_WRITERECOVERY=0 "

        if args.wma_preload:
            xrdcpCmd = "%s xrdcp" % args.wma_preload
            self.xrdfsCmd = "%s xrdfs" % args.wma_preload
        else:
            xrdcpCmd = "xrdcp"
            self.xrdfsCmd = "xrdfs"

        # Check for auth-related environment to prepend
        authEnv = self._getAuthEnv(authMethod, forceMethod)
        if authEnv:
            copyCommand += authEnv
            self.xrdfsCmd = "%s %s" % (authEnv, self.xrdfsCmd)
        copyCommand += "%s --force --nopbar " % xrdcpCmd
        
        if copyCommandOptions:
            copyCommand += "%s " % copyCommandOptions

        if useChecksum:
            checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)
            copyCommand += "--cksum adler32:%s " % checksums['adler32']

        copyCommand += " \"%s\" " % sourcePFN
        copyCommand += " \"%s\" \n" % targetPFN
        copyCommand += "RC=$? \n"
        copyCommand += "echo \"xrdcp exit code: $RC\" \n"
        if self.stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"

        (_, host, path, _) = self.splitPFN(remotePFN)

        if self.stageIn:
            removeCommand = ""
        else:
            removeCommand = "%s '%s' rm %s ;" % (self.xrdfsCmd, host, path)

        copyCommand += "REMOTE_SIZE=`%s '%s' stat '%s' | grep Size | sed -r 's/.*Size:[ ]*([0-9]+).*/\\1/'`\n" % (self.xrdfsCmd, host, path)
        copyCommand += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"

        if useChecksum:

            copyCommand += "echo \"Local File Checksum is: %s\"\n" % checksums['adler32']
            copyCommand += "REMOTE_XS=`%s '%s' query checksum '%s' | grep -i adler32 | sed -r 's/.*[adler|ADLER]32[ ]*([0-9a-fA-F]{8}).*/\\1/'`\n" % (self.xrdfsCmd, host, path)
            copyCommand += "echo \"Remote File Checksum is: $REMOTE_XS\"\n"

            copyCommand += "if [ $RC == 0 ] && [ $REMOTE_SIZE ] && [ $REMOTE_XS ] && [ $LOCAL_SIZE == $REMOTE_SIZE ] && [ '%s' == $REMOTE_XS ]; then exit 0; " % checksums['adler32']
            copyCommand += "else echo \"ERROR: XRootD file transfer return code is $RC. Size or Checksum Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand

        else:

            copyCommand += "if [ $RC == 0 ] && [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
            copyCommand += "else echo \"ERROR: XRootD file transfer return code is $RC. Size or Checksum Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand

        if dryRun:
            return self.debuggingTemplate.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)

        return copyCommand

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        _createStageOutCommand_

        Build the actual xrdcp stageout command

        :sourcePFN: str, the source PFN.
        :targetPFN: str, the target PFN.
        :options: str, additional options for gfal-copy.
        :checksums: dict, checksum values.
        :authMethod: str, preferred authentication method.
        :forceMethod: bool, whether to force the preferred authentication method.
        :dryRun: bool, dry run mode (to enable debug mode).

        """
        # Construct xrdcp stageout command and return it
        return self.getFormattedCopyCommand(sourcePFN, targetPFN, options, checksums, authMethod, forceMethod)
    
    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed xrdcp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        :authMethod: str, the authentication method to be preferentially used ("X509", "TOKEN", or None)
        :forceMethod: bool, whether to force the use of the preferred authentication method, disabling the other 
        """
        # Construct xrdcp/xrdfs commands, but returning the debugging information instead of running it
        return self.getFormattedCopyCommand(sourcePFN, targetPFN, options, checksums, authMethod, forceMethod, dryRun=True)

    def removeFile(self, pfnToRemove):
        """
        Remove a file
        :pfnToRemove: str, PFN of the source file
        """
        (_, host, path, _) = self.splitPFN(pfnToRemove)

        # Try TOKEN authentication first, follow with safe-logic auth methods otherwise
        authEnv = self._getAuthEnv(authMethod="TOKEN")
        command = "%s %s %s rm %s" % (authEnv, self.xrdfsCmd, host, path)

        try:
            self.executeCommand(command)
            logging.info("Removing file with TOKEN authentication succeeded (X509 still enabled).")
        except StageOutError as ex:
            logging.error("Removing file with TOKEN authentication failed. Command: %s", command)
            logging.error("Error: %s", str(ex))
            logging.info("Attempting to remove file with authentication safe-logic")
            if os.getenv("X509_USER_PROXY"):
                logging.info("Retrying with X509_USER_PROXY with BEARER_TOKEN unset...")
                authEnv = self._getAuthEnv(authMethod="X509", forceMethod=True)
                command = "%s %s %s rm %s" % (authEnv, self.xrdfsCmd, host, path)
                try:
                    self.executeCommand(command)
                    logging.info("removefile succeeded with X509 with BEARER_TOKEN unset.")
                    return
                except StageOutError as fallbackEx:
                    logging.error("Fallback with X509_USER_PROXY failed:\n%s", str(fallbackEx))

            if os.getenv("BEARER_TOKEN") or os.getenv("BEARER_TOKEN_FILE"):
                logging.info("Retrying with BEARER_TOKEN with X509_USER_PROXY unset...")
                authEnv = self._getAuthEnv(authMethod="TOKEN", forceMethod=True)
                command = "%s %s %s rm %s" % (authEnv, self.xrdfsCmd, host, path)
                try:
                    self.executeCommand(command)
                    logging.info("Removing file succeeded with TOKEN with X509_USER_PROXY unset.")
                    return
                except StageOutError as fallbackEx:
                    logging.error("Fallback with BEARER_TOKEN failed:\n%s", str(fallbackEx))
              
        return

registerStageOutImpl("xrdcp", XRDCPImpl)
