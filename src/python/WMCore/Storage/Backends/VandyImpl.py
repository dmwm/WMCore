#!/usr/bin/env python
"""
_HadoopImpl_

Implementation of StageOutImpl interface for Vanderbilt

"""

import os.path
import logging

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


class VandyImpl(StageOutImpl):
    '''
    _VandyImp_

    Implement the vandy interface
    '''

    # BASEDIR='/Users/brumgard/Documents/workspace/VandyStageOut/src/scripts'
    BASEDIR = '/usr/local/cms-stageout'

    def __init__(self, stagein=False):

        StageOutImpl.__init__(self, stagein)

        self._mkdirScript = os.path.join(VandyImpl.BASEDIR, 'vandyMkdir.sh')
        self._cpScript = os.path.join(VandyImpl.BASEDIR, 'vandyCp.sh')
        self._rmScript = os.path.join(VandyImpl.BASEDIR, 'vandyRm.sh')
        self._downloadScript = os.path.join(VandyImpl.BASEDIR, 'vandyDownload.sh')

    def createSourceName(self, protocol, pfn):

        """
        _createSourceName_

        uses pfn

        """

        return "%s" % pfn

    def createOutputDirectory(self, targetPFN):

        """
        _createOutputDirectory_

        Creates the directory for vanderbilt
        """

        command = "%s %s" % (self._mkdirScript, os.path.dirname(targetPFN))

        # Calls the parent execute command to invoke the script which should
        # throw a stage out error
        self.executeCommand(command)

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):

        """
        _createStageOutCommand_

        Build a shell command that will transfer the sourcePFN to the
        targetPFN.

        """
        logging.warning("Warning! VandyImpl does not support authMethod handling")

        if self.stageIn:
            return "%s %s %s" % (self._downloadScript, sourcePFN, targetPFN)
        else:
            return "%s %s %s" % (self._cpScript, sourcePFN, targetPFN)
        
    def createDebuggingCommand(self, sourcePFN, targetPFN, options=None, checksums=None, authMethod=None, forceMethod=False):
        """
        Debug a failed vandy copy command for stageOut, without re-running it,
        providing information on the environment and the certifications

        :sourcePFN: str, PFN of the source file
        :targetPFN: str, destination PFN
        :options: str, additional options for copy command
        :checksums: dict, collect checksums according to the algorithms saved as keys
        """
        # Build the command for debugging purposes
        copyCommand = ""
        if self.stageIn:
            copyCommand += "%s %s %s" % (self._downloadScript, sourcePFN, targetPFN)
        else:
            copyCommand += "%s %s %s" % (self._cpScript, sourcePFN, targetPFN)

        result = self.debuggingTemplate.format(copy_command=copyCommand, source=sourcePFN, destination=targetPFN)
        return result

    def removeFile(self, pfnToRemove):

        """
        _removeFile_

        Removes the pfn.
        """

        command = "%s %s" % (self._rmScript, pfnToRemove)

        self.executeCommand(command)


# Registers the implementation
registerStageOutImpl("vandy", VandyImpl)
