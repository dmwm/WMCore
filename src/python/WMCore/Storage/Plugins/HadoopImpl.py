#!/usr/bin/env python
"""
_HadoopImpl_

Implementation of StageOutImpl interface for Hadoop

"""
import os
import logging

from WMCore.Storage.Registry import registerStageOutImplVersionTwo
from WMCore.Storage.StageOutImplV2 import StageOutImplV2

from WMCore.Storage.Execute import runCommandWithOutput as runCommand
from WMCore.Storage.StageOutError import StageOutError

class HadoopImpl(StageOutImplV2):
    """
    _HadoopImpl_

    Implement interface for Hadoop
    
    """
    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        original_size = os.stat(fromPfn)[6]
        print "Local File Size is: %s" % original_size
        commandArgs = ["hadoop","fs","-put"]
        endingCommand = [fromPfn, toPfn]
        if options != None:
            commandArgs.extend(options.split())
        commandArgs.extend(endingCommand)
        (exitCode, output) = runCommand(commandArgs)
        if not exitCode:
            logging.error("Error in hadoop transfer:")
            logging.error(output)
            raise StageOutError, "Transfer failure"
      
        return toPfn

    
    def doDelete(self, pfn, seName, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        runCommand(["hadoop", "fs", "-rm" ,"pfnToRemove"])


    

registerStageOutImplVersionTwo("hadoop", HadoopImpl)
