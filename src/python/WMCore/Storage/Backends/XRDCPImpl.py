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


class XRDCPImpl(StageOutImpl):
    """
    _XRDCPImpl_

    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300
        self.xrdfsCmd = "xrdfs"

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

    def _checkXRDUtilsExist(self):
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

    def getAuthEnv(self, authmethod=None, forcemethod=False):
        """
        Get environment variables for stageout command based on the selected authentication method.
        :authmethod: str, the authentication method to be used ("X509", "TOKEN", or None)
        :forcemethod: bool, cleans non-chosen auth methods from environment.
        :return: str
        """
        authEnv = ""
        if authmethod == "X509":
            authEnv = "env X509_USER_PROXY=$X509_USER_PROXY "
            if forcemethod:
                authEnv += "BEARER_TOKEN_FILE= BEARER_TOKEN= "
        elif authmethod == "TOKEN":
            authEnv = "env BEARER_TOKEN_FILE=$BEARER_TOKEN_FILE BEARER_TOKEN=$(cat $BEARER_TOKEN_FILE) "
            if forcemethod:
                authEnv += "X509_USER_PROXY= "
        else:
            logging.info("Warning! Running without either a X509 certificate or a token specified!")

        return authEnv

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authmethod=None, forcemethod=False):
        """
        _createStageOutCommand_

        Build the actual xrdcp stageout command

        If adler32 checksum is provided, use it for the transfer
        xrdcp options used:
          --force : re-creates a file if it's already present
          --nopbar : does not display the progress bar

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
        if not self._checkXRDUtilsExist():
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
        authEnv = self.getAuthEnv(authmethod, forcemethod)
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

        return copyCommand
    
    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        Debug a failed xrdcp command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommandDict = self.buildCopyCommandDict(sourcePFN, targetPFN, options, checksums)
        copyCommand = self.copyCommand.format_map(copyCommandDict)

        result = self.debuggingTemplate.format(copy_command=copyCommand, source=copyCommandDict['source'], destination=copyCommandDict['destination'])
        return result

    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authmethod=None, forcemethod=False):
        """
        Debug a failed xrdcp/xrdfs command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for gfal-cp
        :checksums: dict, collect checksums according to the algorithms saved as keys
        :authmethod: str, the authentication method to be used ("X509", "TOKEN", or None)
        :forcemethod: bool, cleans non-chosen auth methods from environment.
        """
        copyCommand = self.createStageOutCommand(sourcePFN, targetPFN, options, checksums, authmethod, forcemethod)

        result = "#!/bin/bash\n"
        result += """
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
        """.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)
        return result

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        """
        (_, host, path, _) = self.splitPFN(pfnToRemove)
        command = "%s %s rm %s" % (self.xrdfsCmd, host, path)
        self.executeCommand(command)
        return

registerStageOutImpl("xrdcp", XRDCPImpl)
