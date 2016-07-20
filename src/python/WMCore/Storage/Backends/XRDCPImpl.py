#!/usr/bin/env python
"""
_XRDCPImpl_

Implementation of StageOutImpl interface for xrdcp

Generic, will/should work with any site.

"""
from __future__ import print_function
import os
import argparse

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

from WMCore.Storage.Execute import execute


class XRDCPImpl(StageOutImpl):
    """
    _XRDCPImpl_

    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300

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

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
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
        parser.add_argument('--cerncastor', action='store_true')
        args, unknown = parser.parse_known_args(options.split())

        copyCommandOptions = ' '.join(unknown)

        copyCommand = ""

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            if args.cerncastor:
                targetPFN += "?svcClass=t0cms"

        useChecksum = (checksums != None and 'adler32' in checksums and not self.stageIn)

        copyCommand += "xrdcp --force --nopbar "

        if copyCommandOptions:
            copyCommand += "%s " % copyCommandOptions

        if useChecksum:
            checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)
            copyCommand += "--cksum adler32:%s " % checksums['adler32']

        copyCommand += " \"%s\" " % sourcePFN
        copyCommand += " \"%s\" \n" % targetPFN

        if self.stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"

        (_, host, path, _) = self.splitPFN(remotePFN)

        if self.stageIn:
            removeCommand = ""
        else:
            removeCommand = "xrdfs %s rm %s" % (host, path)

        copyCommand += "REMOTE_SIZE=`xrdfs '%s' stat '%s' | grep Size | sed -r 's/.*Size:[ ]*([0-9]+).*/\\1/'`\n" % (host, path)
        copyCommand += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"

        if useChecksum:

            copyCommand += "echo \"Local File Checksum is: %s\"\n" % checksums['adler32']
            copyCommand += "REMOTE_XS=`xrdfs '%s' query checksum '%s' | grep adler32 | sed -r 's/.*adler32[ ]*([0-9a-fA-F]{8}).*/\\1/'`\n" % (host, path)
            copyCommand += "echo \"Remote File Checksum is: $REMOTE_XS\"\n"

            copyCommand += "if [ $REMOTE_SIZE ] && [ $REMOTE_XS ] && [ $LOCAL_SIZE == $REMOTE_SIZE ] && [ '%s' == $REMOTE_XS ]; then exit 0; " % checksums['adler32']
            copyCommand += "else echo \"Error: Size or Checksum Mismatch between local and SE\"; %s ; exit 60311 ; fi" % removeCommand

        else:

            copyCommand += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
            copyCommand += "else echo \"Error: Size Mismatch between local and SE\"; %s ; exit 60311 ; fi" % removeCommand

        return copyCommand

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        """
        (_, host, path, _) = self.splitPFN(pfnToRemove)
        command = "xrdfs %s rm %s" % (host, path)
        execute(command)
        return

registerStageOutImpl("xrdcp", XRDCPImpl)
