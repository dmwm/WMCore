#!/usr/bin/env python
"""
_LCGImpl_

Implementation of StageOutImplV2 interface for lcg-cp

"""
import os, re, logging, subprocess

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class LCGImpl(StageOutImplV2):
    """
    _LCGImpl_

    Implement interface for srmcp v2 command with lcg-* commands
    
    """
        
    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        localFileName = fromPfn
        fromPfn = self.prependFileProtocol(fromPfn)    
        transferCommand = "lcg-cp -b -D srmv2 --vo cms -t 2400 --verbose %s %s %s " %\
                            ( options, fromPfn, toPfn )
        
        logging.info("Staging out with lcg-cp")
        logging.info("  commandline: %s" % transferCommand)
        (exitCode, output) = runCommand(transferCommand)
        # riddle me this, the following line fails with:
        # not all arguments converted during string formatting
        #FIXME
        logging.info("  output from lcg-cp: %s" % output)
        logging.info("  complete. #" )#exit code" is %s" % exitCode)
    
        logging.info("Verifying file sizes")
        localSize  = os.path.getsize( localFileName )
        remoteSize = subprocess.Popen(['lcg-ls', '-l', '-b', '-D', 'srmv2', toPfn],
                                       stdout=subprocess.PIPE).communicate()[0]
        logging.info("got the following from lcg-ls %s" % remoteSize)
        remoteSize = remoteSize.split()[4]
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
        if pfn.startswith("srm://"):
            runCommand( "lcg-del -b -l -D srmv2 --vo cms %s" % pfn )
        elif pfn.startswith("file:"):
            runCommand( "/bin/rm -f %s" % pfn.replace("file:", "", 1) )
        else:
            runCommand( StageOutImpl.createRemoveFileCommand(self, pfn) )

   



