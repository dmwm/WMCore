#!/usr/bin/env python
"""
_PYDCCPImpl_

Implementation of StageOutImpl interface for DCCP With PyDCAP bindings
available

"""
import os
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

_CheckExitCodeOption = True



class PYDCCPImpl(StageOutImpl):
    """
    _PYDCCPImpl_

    Implement interface for python API around dccp command
    
    """        
        
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

        Build a dccp command with a pnfs mkdir to generate the directory

        """
        try:
            import dcap
        except ImportError, ie:
            raise StageOutError("Python dCap wrappers not found on this host.")
        
        optionsStr = ""
        if options != None:
            optionsStr = str(options)
        result = "#!/bin/sh\n"
        result += "dc_stageout %s %s %s" % ( optionsStr, sourcePFN, targetPFN)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        TODO: Implement cleanup of file via this API

        """
        pass

registerStageOutImpl("pydcap", PYDCCPImpl)
