#!/usr/bin/env python
"""
_CPImpl_

Implementation of StageOutImpl interface for plain cp

Useful for sites with large shared POSIX file systems (HPC)
"""
from __future__ import division

import os
import errno

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


class CPImpl(StageOutImpl):
    """
    _CPImpl_

    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        """
        return pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create output directory if needed
        """
        targetdir = os.path.dirname(targetPFN)

        if not os.path.isdir(targetdir):
            try:
                os.makedirs(targetdir)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        return

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):
        """
        _createStageOutCommand_

        Build the actual cp stageout command

        """
        copyCommand = ""

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            copyCommand += "LOCAL_SIZE=`stat -c%%s %s`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"

        copyCommand += "cp %s %s\n" % (sourcePFN, targetPFN)

        if self.stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s %s`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            removeCommand = ""
        else:
            removeCommand = "rm %s;" % remotePFN

        copyCommand += "REMOTE_SIZE=`stat -c%%s %s`\n" % remotePFN
        copyCommand += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"

        copyCommand += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
        copyCommand += "else echo \"ERROR: Size Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand

        return copyCommand

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        """
        os.remove(pfnToRemove)
        return

registerStageOutImpl("cp", CPImpl)
