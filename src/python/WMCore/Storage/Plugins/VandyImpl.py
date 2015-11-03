#!/usr/bin/env python
"""
_VandyImpl_

Implementation of StageOutImpl interface for Vanderbilt

"""
from __future__ import print_function

import os.path

from WMCore.Storage.StageOutImplV2 import StageOutImplV2

from WMCore.Storage.Execute import runCommandWithOutput as runCommand
from WMCore.Storage.StageOutError import StageOutError
import logging

class VandyImpl(StageOutImplV2):


    #BASEDIR='/Users/brumgard/Documents/workspace/VandyStageOut/src/scripts'
    BASEDIR='/usr/local/cms-stageout'

    def __init__(self, stagein=False):

        StageOutImplV2.__init__(self)

        self._mkdirScript    = os.path.join(VandyImpl.BASEDIR, 'vandyMkdir.sh')
        self._cpScript       = os.path.join(VandyImpl.BASEDIR, 'vandyCp.sh')
        self._rmScript       = os.path.join(VandyImpl.BASEDIR, 'vandyRm.sh')
        self._downloadScript = os.path.join(VandyImpl.BASEDIR, 'vandyDownload.sh')


    def createOutputDirectory(self, targetPFN):

        """
        _createOutputDirectory_

        Creates the directory for vanderbilt
        """

        command = "%s %s" % (self._mkdirScript, targetPFN)

        # Calls the parent execute command to invoke the script which should
        # throw a stage out error
        exitCode, output = runCommand(command)

        if exitCode != 0:
            logging.error("Error creating directory in LStore")
            logging.error("Command: %s" % command)
            logging.error(output)


    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum):
        """
            if stageOut is true:
                The fromPfn is the LOCAL FILE NAME on the node, without file://
                the toPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            if stageOut is false:
                The toPfn is the LOCAL FILE NAME on the node, without file://
                the fromPfn is the source PFN, mapped from the LFN using the TFC or overrrides
        """

        # Figures out the src and dst files
        if stageOut:
            dstPath = toPfn
        else:
            dstPath = fromPfn

        # Creates the directory
        if stageOut:
            self.createOutputDirectory(os.path.dirname(dstPath))
        else:
            os.makedirs(os.path.dirname(dstPath))

        # Does the copy
        if stageOut:
            command = "%s %s %s" % (self._cpScript, fromPfn, toPfn)
        else:
            command = "%s %s %s" % (self._downloadScript, fromPfn, toPfn)

        exitCode, output = runCommand(command)

        print(output)

        if exitCode != 0:
            logging.error("Error in file transfer:")
            logging.error(output)
            raise StageOutError("Transfer failure, command %s, error %s" % (command, output))

        # Returns the path
        return dstPath


    def doDelete(self, pfn, pnn, command, options, protocol  ):
        """
        _removeFile_

        Removes the pfn.
        """

        command = "%s %s" % (self._rmScript, pfn)

        exitCode, output = runCommand(command)

        if exitCode != 0:
            logging.error("Error removing file from LStore")
            logging.error(output)
            raise StageOutError("remove file failure command %s, error %s" % (command, output))
