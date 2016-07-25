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




class TestFailImpl(StageOutImplV2):
    """
    _FailImpl_

    Test plugin that always results in a StageOutFailure

    """

    def doTransfer(self, lfn, pfn, stageOut, pnn, command, options, protocol, checksum  ):
        raise StageOutFailure("FailImpl returns FAIL!!!")


    def doDelete(self, lfn, pfn, pnn, command, options, protocol  ):
        raise StageOutFailure("FailImpl returns FAIL!!!")
