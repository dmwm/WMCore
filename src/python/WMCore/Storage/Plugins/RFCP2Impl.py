#!/usr/bin/env python
"""
_RFCP2Impl_

Implementation of StageOutImpl interface for RFIO in Castor-2

"""
import os 

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.Execute import runCommandWithOutput as runCommand
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure
from WMCore.Storage.Execute import runCommand
import logging
from subprocess import Popen, PIPE


class RFCP2Impl(StageOutImplV2):
    """
    _RFCP2Impl_

    Implement interface for rfcp command
    
    """


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """
        
        targetdir= os.path.dirname(targetPFN)
        needToMkdir = False
        try:
            self.runCommandFailOnNonZero(['rfstat', targetdir])
        except:
            needToMkdir = True
            
        if needToMkdir:
            logging.info('Creating directory %s' % targetdir)
            self.runCommandWarnOnNonZero(['rfmkdir', '-m', '775', '-p',targetdir])


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
                        ["rfcp"],
                        [fromPfn, toPfn],
                        options)
        self.runCommandFailOnNonZero(ourCommand)
        
        # keeping this logic though I don't believe in it
        # AMM -7/13/2010
        if not stageOut:
            remotePFN, localPFN = fromPfn, toPfn
        else:
            remotePFN, localPFN = toPfn, fromPfn

        localSize  = os.path.getsize( localPFN )                
        p1 = Popen(["rfstat", remotePFN], stdout=PIPE)
        p2 = Popen(["grep", "Size"], stdin=p1.stdout, stdout=PIPE)
        p3 = Popen(['cut','-f2','-d'], stdin=p2.stdout, stdout=PIPE)
        remoteSize = p3.communicate()[0]
        logging.info("Localsize: %s Remotesize: %s" % (localSize, remoteSize))
        if int(localSize) != int(remoteSize):
            try:
                self.doDelete(toPfn,None,None,None,None)
            except:
                pass
            raise StageOutFailure, "File sizes don't match"
        
        return toPfn


    def doDelete(self, pfn, seName, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """

        runCommand(["stageclr", "-M", pfn])
        runCommand(["nsrm", pfn])


