#!/usr/bin/env python
"""
_HadoopImpl_

Implementation of StageOutImpl interface for Vanderbilt

"""

import os.path

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

    def createStageOutCommand(self, sourcePFN, targetPFN, options=None, checksums=None):

        """
        _createStageOutCommand_

        Build a shell command that will transfer the sourcePFN to the
        targetPFN.

        """
        if self.stageIn:
            return "%s %s %s" % (self._downloadScript, sourcePFN, targetPFN)
        else:
            return "%s %s %s" % (self._cpScript, sourcePFN, targetPFN)

    def removeFile(self, pfnToRemove):

        """
        _removeFile_

        Removes the pfn.
        """

        command = "%s %s" % (self._rmScript, pfnToRemove)

        self.executeCommand(command)


# Registers the implementation
registerStageOutImpl("vandy", VandyImpl)
