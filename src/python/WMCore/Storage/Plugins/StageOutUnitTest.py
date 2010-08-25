#!/usr/bin/env python
"""
_TestImpl_

Couple of test implementations for unittests

"""

import os
import os.path

from WMCore.Storage.Registry import registerStageOutImplVersionTwo
from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutFailure

class WinImpl(StageOutImplV2):
    """
    _WinImpl_

    Test plugin that always returns success

    """
    def doTransfer(self, lfn, pfn, stageOut, seName, command, options, protocol  ):
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

        
    def createSourceName(self, protocol, pfn):
        """
        This should return the same PFN
        """
        return pfn


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        command = "cp %s %s" %(sourcePFN, sourcePFN+'2')
        return command


    def removeFile(self, pfnToRemove):
        command = "rm  %s" %pfnToRemove
        self.executeCommand(command)
        return "WIN!!!"


registerStageOutImplVersionTwo("test-win", WinImpl)
registerStageOutImplVersionTwo("test-fail", FailImpl)
registerStageOutImplVersionTwo("test-copy", LocalCopyImpl)
