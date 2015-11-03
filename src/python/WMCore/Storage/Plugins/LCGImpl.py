#!/usr/bin/env python
"""
_LCGImpl_

Implementation of StageOutImplV2 interface for lcg-cp

"""
import os, os.path, re, logging, subprocess, tempfile
from subprocess import Popen

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure
from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class LCGImpl(StageOutImplV2):
    """
    _LCGImpl_

    Implement interface for srmcp v2 command with lcg-* commands

    """

    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        localFileName = fromPfn
        if stageOut:
            fromPfn2 = self.prependFileProtocol(fromPfn)
            toPfn2 = toPfn
            localFileName = fromPfn
            remoteFileName = toPfn
        else:
            fromPfn2 = fromPfn
            toPfn2 = self.prependFileProtocol(toPfn)
            localFileName = toPfn
            remoteFileName = fromPfn
            localDir = os.path.dirname( localFileName )
            if not os.path.exists( localDir ):
                logging.info("Making local directory %s" % localDir)
                os.makedirs( localDir )

        if not options:
            options = ""

        transferCommand = "lcg-cp -b -D srmv2 --vo cms --srm-timeout 2400 --sendreceive-timeout 2400 --connect-timeout 300 --verbose %s %s %s " %\
                            ( options, fromPfn2, toPfn2 )

        logging.info("Staging out with lcg-cp")
        logging.info("  commandline: %s" % transferCommand)
        self.runCommandFailOnNonZero( transferCommand )

        logging.info("Verifying file sizes")
        localSize  = os.path.getsize( localFileName )
        remoteSize = subprocess.Popen(['lcg-ls', '-l', '-b', '-D', 'srmv2', remoteFileName],
                                       stdout=subprocess.PIPE).communicate()[0]
        logging.info("got the following from lcg-ls %s" % remoteSize)
        remoteSize = remoteSize.split()[4]
        logging.info("Localsize: %s Remotesize: %s" % (localSize, remoteSize))
        if int(localSize) != int(remoteSize):
            try:
                logging.error("Transfer failed, deleting partial file")
                self.doDelete(toPfn,None,None,None,None)
            except:
                pass
            raise StageOutFailure("File sizes don't match")


        return toPfn


    def doDelete(self, pfn, pnn, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        if pfn.startswith("srm://"):
            runCommand( "lcg-del -b -l -D srmv2 --vo cms %s" % pfn )
        elif pfn.startswith("file:"):
            runCommand( "/bin/rm -f %s" % pfn.replace("file:", "", 1) )
        else:
            runCommand( StageOutImpl.createRemoveFileCommand(self, pfn) )
