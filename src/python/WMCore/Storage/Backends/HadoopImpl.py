#!/usr/bin/env python
"""
_HadoopImpl_

Implementation of StageOutImpl interface for Hadoop

"""
import os

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class HadoopImpl(StageOutImpl):
    """
    _HadoopImpl_

    Implement interface for Hadoop
    
    """

    run = staticmethod(runCommand)

    def createSourceName(self, _, pfn):
        """
        _createSourceName_

        uses pfn

        """
        return "%s" % pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        This is a no-op, as Hadoop automatically creates directories.
        """


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an Hadoop put

        """
        original_size = os.stat(sourcePFN)[6]
        print "Local File Size is: %s" % original_size
        result = "hadoop fs -put "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "hadoop fs -rm %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("hadoop", HadoopImpl)
