#!/usr/bin/env python
"""
_CPImpl_

Implementation of StageOutImpl interface for plain cp

"""
from __future__ import division

import os
import errno
import shutil

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutFailure


class CPImpl(StageOutImplV2):
    """
    _CPImpl_

    Implement interface for plain cp command

    """

    def createOutputDirectory(self, targetPFN):
        targetdir = os.path.dirname(targetPFN)
        if not os.path.isdir(targetdir):
            try:
                os.makedirs(targetdir)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        return

    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum ):
        self.createOutputDirectory(toPfn)
        shutil.copy(fromPfn, toPfn)
        if os.path.getsize(fromPfn) != os.path.getsize(toPfn):
            raise StageOutFailure("Invalid file size")
        return toPfn

    def doDelete(self, pfn, pnn, command, options, protocol):
        os.remove(pfn)
        return
