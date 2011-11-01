#!/usr/bin/env python
"""
_VandyImpl_

Implementation of StageOutImpl interface for Vanderbilt

"""

import os.path

from WMCore.Storage.StageOutImplV2 import StageOutImplV2

from WMCore.Storage.Execute import runCommandWithOutput as runCommand
from WMCore.Storage.StageOutError import StageOutError
import logging

class VandyImpl(StageOutImplV2):
    
    
    #BASEDIR='/Users/brumgard/Documents/workspace/VandyStageOut/src/scripts'
    BASEDIR='/usr/local/cms-stageout'
    
    def __init__(self, stagein=False):
        
        StageOutImplV2.__init__(self)
        
        self._mkdirScript = os.path.join(VandyImpl.BASEDIR, 'vandyMkdir.sh')
        self._cpScript    = os.path.join(VandyImpl.BASEDIR, 'vandyCp.sh')
        self._rmScript    = os.path.join(VandyImpl.BASEDIR, 'vandyRm.sh')
    
    
    def createOutputDirectory(self, targetPFN):
        
        """
        _createOutputDirectory_

        Creates the directory for vanderbilt
        """
        
        command = "%s %s" % (self._mkdirScript, os.path.dirname(targetPFN))
        
        # Calls the parent execute command to invoke the script which should 
        # throw a stage out error
        exitCode, output = runCommand(command)
        
        if exitCode != 0:
            logging.error("Error creating directory")
            logging.error(output)
    
    
    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, 
                   protocol):
        
        # Figures out the src and dst files
        if stageOut:
            srcPath = fromPfn
            dstPath = toPfn
        else:
            srcPath = toPfn
            dstPath = fromPfn
        
        # Creates the directory
        self.createOutputDirectory(os.path.dirname(dstPath))
        
        # Does the copy
        command = "%s %s %s" % (self._cpScript, srcPath, dstPath)
        
        exitCode, output = runCommand(command)
    
        print(output)
    
        if exitCode != 0:
            logging.error("Error in file transfer:")
            logging.error(output)
            raise StageOutError, "Transfer failure"
    
        # Returns the path
        return dstPath
    
    
    def doDelete(self, pfn, seName, command, options, protocol  ):
        """
        _removeFile_

        Removes the pfn.
        """
        
        command = "%s %s" % (self._rmScript, pfn)
        
        exitCode, output = runCommand(command)
    
        if exitCode != 0:
            logging.error("Error removing file")
            logging.error(output)
            raise StageOutError, "remove file failure"
    
    