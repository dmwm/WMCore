#!/usr/bin/env python
"""
_TestImpl_

Couple of test implementations for unittests

"""

import os
import os.path
import shutil

from WMCore.Storage.Registry import registerStageOutImplVersionTwo
from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutFailure

class WinImpl(StageOutImplV2):
    """
    _WinImpl_

    Test plugin that always returns success

    """
    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        return "WIN!!!"

    def doDelete(self, lfn, pfn, seName, command, options, protocol  ):
        pass




class FailImpl(StageOutImplV2):
    """
    _FailImpl_

    Test plugin that always results in a StageOutFailure

    """

    def doTransfer(self, lfn, pfn, stageOut, seName, command, options, protocol  ):
        raise StageOutFailure("FailImpl returns FAIL!!!")


    def doDelete(self, lfn, pfn, seName, command, options, protocol  ):
        raise StageOutFailure("FailImpl returns FAIL!!!")


class LocalCopyImpl(StageOutImplV2):
    """
    _LocalCopyImp_

    Test plugin that copies to a local directory

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



registerStageOutImplVersionTwo("test-win", WinImpl)
registerStageOutImplVersionTwo("test-fail", FailImpl)
registerStageOutImplVersionTwo("test-copy", LocalCopyImpl)
