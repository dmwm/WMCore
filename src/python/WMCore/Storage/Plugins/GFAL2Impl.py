#!/usr/bin/env python
"""
_GFAL2Impl_
Implementation of StageOutImpl interface for gfal-copy
"""
import os, logging

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure
from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True


class GFAL2Impl(StageOutImpl2):
    """
    _GFAL2Impl_
    Implement interface for GFAL2 commands (gfal-copy, gfal-rm)
    """

    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol, checksum):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn
            or raises on failure. StageOutError (and inherited exceptions) are for expected errors
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
            localDir = os.path.dirname(localFileName)

        if not options:
            options = ""

        transferCommand = "env -i X509_USER_PROXY=$X509_USER_PROXY gfal-copy -t 2400 -T 2400 -p -K adler32 -vvv %s %s %s " %\
                            (options, fromPfn2, toPfn2)

        logging.info("Staging out with gfal-copy")
        logging.info("  commandline: %s" % transferCommand)
        commandExec = self.runCommandFailOnNonZero(transferCommand)

        if commandExec[0] != 0:
            try:
                logging.error("Transfer failed, deleting partial file")
                self.doDelete(toPfn, None, None, None, None)
            except:
                pass
            raise StageOutFailure("File transfer failed")

        return toPfn


    def doDelete(self, pfn, seName, command, options, protocol):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        if os.path.isfile(pfn):
            runCommand("/bin/rm -f %s" % pfn)
        elif pfn.startswith("file:"):
            runCommand("env -i X509_USER_PROXY=$X509_USER_PROXY gfal-rm -vvv %s" % pfn)
        else:
            runCommand(StageOutImpl.createRemoveFileCommand(self, pfn))
