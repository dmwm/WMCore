#!/usr/bin/env python
"""
_CPImpl_

Implementation of StageOutImpl interface for plain cp

"""
import os
import shutil
import os.path

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutFailure


class CPImpl(StageOutImplV2):
    """
    _CPImpl_

    Implement interface for plain cp command
    
    """

    def createOutputDirectory(self, targetPFN):
        """
        I guess this masks a directory?

        """
        
        dirName = os.path.dirname(targetPFN)

        if not os.path.isdir(dirName):
            os.makedirs(dirName)

        return
    
    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        self.createOutputDirectory( toPfn )
        shutil.copy(fromPfn, toPfn)
        if os.path.getsize(fromPfn) != os.path.getsize(toPfn):
            raise StageOutFailure, "Invalid file size"
        return toPfn


    def doDelete(self, pfn, seName, command, options, protocol  ):
        os.unlink(pfn)



