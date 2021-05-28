#!/usr/bin/env python
"""
_StageOutImplV2_

Interface for Stage Out Plugins. All stage out implementations should
inherit this object and implement the methods accordingly

"""

from builtins import object

from WMCore.Storage.Execute import runCommandWithOutput as runCommand
from WMCore.Storage.StageOutError import StageOutError
import logging
import os

class StageOutImplV2(object):
    """
    _StageOutImplV2_

    """

    def __init__(self):
        self.numRetries = 5
        pass

    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin

            if stageOut is true:
                The fromPfn is the LOCAL FILE NAME on the node, without file://
                the toPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            if stageOut is false:
                The toPfn is the LOCAL FILE NAME on the node, without file://
                the fromPfn is the source PFN, mapped from the LFN using the TFC or overrrides

            this behavior is because most transfer commands will switch their direction
            simply by swapping the order of the arguments. the stageOut flag is provided
            however, because sometimes you want to pass different command line args

        """
        raise NotImplementedError

    def doDelete(self, pfn, pnn, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        raise NotImplementedError

    def prependFileProtocol(self, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if os.path.isfile(pfn):
            return "file:%s" % pfn
        else:
            return pfn

    def runCommandFailOnNonZero(self, command):
        logging.info("Executing %s", command)
        (exitCode, output) = runCommand(command)
        if exitCode:
            logging.error("Error in file transfer:")
            logging.error("  Command executed was: %s", command )
            logging.error("  Output was: %s", output )
            logging.error("  Exit code was: %s", exitCode )
            raise StageOutError("Transfer failure")
        return (exitCode, output)

    def runCommandWarnOnNonZero(self, command):
        logging.info("Executing %s", command)
        (exitCode, output) = runCommand(command)
        if exitCode:
            logging.error("Error in file transfer..ignoring:")
            logging.error(output)
        return (exitCode, output)

    def generateCommandFromPreAndPostParts(self, pre,post,options):
        """
        simply put, this will return an array with [pre, options.split(), post]
        but if options is empty, won't put it in'
        """
        temp = pre
        if options != None:
            temp.extend(options.split())
        temp.extend(post)
        return temp
