#!/usr/bin/env python
"""
_XRDCPImpl_

Implementation of StageOutImpl interface for RFIO in Castor-2

"""
import os
import logging

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.WMBase import getWMBASE
from subprocess import Popen, PIPE
from WMCore.Storage.Execute import runCommand
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure


class XRDCPImpl(StageOutImplV2):
    """
    _XRDCPImpl_

    Implement interface for rfcp command

    """

    def doWrapped(self, commandArgs):
        wrapperPath = os.path.join(getWMBASE(),'src','python','WMCore','Storage','Plugins','XRDCP','wrapenv.sh')
        commandArgs.insert(0,wrapperPath)
        return runCommand(commandArgs)

    def doTransfer(self, sourcePFN, targetPFN, stageOut, pnn, command, options, protocol, checksum ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        original_size = os.stat(sourcePFN)[6]
        logging.info("Local File Size is: %s" % original_size)
        self.doWrapped(self.generateCommandFromPreAndPostParts(['xrdcp','-s3'],
                                                               [sourcePFN,'root://lxgate39.cern.ch/%s'% targetPFN],
                                                               options))

        p1 = Popen(["rfstat", '-recursion_depth=0','-retry_num=0', targetPFN], stdout=PIPE)
        p2 = Popen(["grep", 'Size'], stdout=PIPE, stdin=p1.stdout)
        p3 = Popen(["cut", '-f2', '-d:'], stdout=PIPE, stdin=p2.stdout)
        remoteSize = p3.communicate()[0]

        if int(original_size) != int(remoteSize):
            try:
                self.doDelete(targetPFN, None, None, None, None)
            except:
                pass
            raise StageOutFailure("File sizes don't match")

    def doDelete(self, pfnToRemove, pnn, command, options, protocol  ):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-1

        """
        self.runCommandWarnOnNonZero(['stager_rm','-M',pfnToRemove])
        self.runCommandWarnOnNonZero(['nsrm',pfnToRemove])
