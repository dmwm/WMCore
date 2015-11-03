#!/usr/bin/env python
"""
_TestImpl_

Couple of test implementations for unittests

"""

import os
import os.path
import shutil


from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutFailure

class TestWinImpl(StageOutImplV2):
    """
    _WinImpl_

    Test plugin that always returns success

    """
    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum  ):
        return "WIN!!!"

    def doDelete(self, lfn, pfn, pnn, command, options, protocol  ):
        pass
