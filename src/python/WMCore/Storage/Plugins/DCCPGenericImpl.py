#!/usr/bin/env python
"""
_DCCPGenericImpl_

Implementation of StageOutImpl interface for DCCP for a generic
site.

Assumes the target dir will already exist.
Doesnt support deletion yet: This needs to be added


"""
import os
import logging

from WMCore.Storage.StageOutImplV2 import StageOutImplV2


_CheckExitCodeOption = True



class DCCPGenericImpl(StageOutImplV2):
    """
    _DCCPGenericImpl_

    Implement interface for dccp command
    
    """

    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
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
                the fromPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            
            this behavior is because most transfer commands will switch their direction
            simply by swapping the order of the arguments. the stageOut flag is provided
            however, because sometimes you want to pass different command line args
                
        """
        ourCommand = \
            self.generateCommandFromPreAndPostParts(\
                        ["dccp"],
                        [fromPfn, toPfn],
                        options)
        self.runCommandFailOnNonZero(ourCommand)

        return toPfn
    
    def doDelete(self, pfn, seName, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        logging.info("Attempted to delete, but delete not supported")
        logging.info(pfn)


    


