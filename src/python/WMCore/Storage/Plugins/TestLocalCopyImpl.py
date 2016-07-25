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


class TestLocalCopyImpl(StageOutImplV2):
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

    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum  ):
        self.createOutputDirectory( toPfn )
        shutil.copy(fromPfn, toPfn)
        if os.path.getsize(fromPfn) != os.path.getsize(toPfn):
            raise StageOutFailure("Invalid file size")
        return toPfn


    def doDelete(self, pfn, pnn, command, options, protocol  ):
        os.unlink(pfn)
