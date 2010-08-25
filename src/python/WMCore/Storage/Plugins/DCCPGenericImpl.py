#!/usr/bin/env python
"""
_DCCPGenericImpl_

Implementation of StageOutImpl interface for DCCP for a generic
site.

Assumes the target dir will already exist.
Doesnt support deletion yet: This needs to be added


"""
import os
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


_CheckExitCodeOption = True



class DCCPGenericImpl(StageOutImpl):
    """
    _DCCPGenericImpl_

    Implement interface for dccp command
    
    """
    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        Create a dir for the target pfn

        ASSUMPTION: Directory already exists 
        
        """
        pass
        
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        dccp takes a local path, so all we have to do is return the
        pfn as-is

        """
        return pfn

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a dccp command 

        """
        optionsStr = ""
        if options != None:
            optionsStr = str(options)
        dirname = os.path.dirname(targetPFN)
        result = "#!/bin/sh\n"
        result += "dccp %s %s %s" % ( optionsStr, sourcePFN, targetPFN)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided:

        Cannot implement this, since there isnt a dcrm command
        
        """
        pass
    

registerStageOutImpl("dccp-generic", DCCPGenericImpl)
