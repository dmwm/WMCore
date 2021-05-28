#!/usr/bin/env python

"""
_Step.Executor.DeleteFiles_

Implementation of an Executor for a Delete step

"""
from __future__ import print_function

from future.utils import viewitems

import signal

from WMCore.Storage.DeleteMgr import DeleteMgr, DeleteMgrError
from WMCore.Storage.FileManager import DeleteMgr as NewDeleteMgr
from WMCore.WMExceptions import WM_JOB_ERROR_CODES
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.Executors.LogArchive import Alarm, alarmHandler


class DeleteFiles(Executor):
    """
    A step run to clean up the unmerged files in a job

    """

    def pre(self, emulator=None):
        """
        _pre_

        Pre execution checks

        """

        # Are we using an emulator?
        if emulator is not None:
            return emulator.emulatePre(self.step)

        self.logger.info("Steps.Executors.%s.pre called", self.__class__.__name__)
        return None

    def execute(self, emulator=None):
        """
        _execute_

        """

        # Are we using emulators again?
        if emulator is not None:
            return emulator.emulate(self.step, self.job)

        self.logger.info("Steps.Executors.%s.execute called", self.__class__.__name__)
        self.logger.info("Step set to numberOfRetries: %s, retryDelay: %s",
                         self.step.retryCount, self.step.retryDelay)

        # Look!  I can steal from StageOut
        # DeleteMgr uses the same manager structure as StageOutMgr

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()
        # Wait up to 5min for a single file deletion
        overrides.setdefault('waitTime', 300)

        self.logger.info("Step with the following overrides:")
        for keyName, value in viewitems(overrides):
            self.logger.info("    %s : %s", keyName, value)

        # Pull out StageOutMgr Overrides
        # switch between old stageOut behavior and new, fancy stage out behavior
        useNewStageOutCode = False
        if 'newStageOut' in overrides and overrides.get('newStageOut'):
            useNewStageOutCode = True

        stageOutCall = {}
        stageOutCall['logger'] = self.logger
        if "command" in overrides and "option" in overrides \
                and "phedex-node" in overrides \
                and "lfn-prefix" in overrides:
            stageOutCall['command'] = overrides.get('command')
            stageOutCall['option'] = overrides.get('option')
            stageOutCall['phedex-node'] = overrides.get('phedex-node')
            stageOutCall['lfn-prefix'] = overrides.get('lfn-prefix')

        # naw man, this is real
        # iterate over all the incoming files
        if not useNewStageOutCode:
            # old style
            manager = DeleteMgr(**stageOutCall)
            manager.numberOfRetries = self.step.retryCount
            manager.retryPauseTime = self.step.retryDelay
        else:
            # new style
            self.logger.critical("DeleteFiles IS USING NEW STAGEOUT CODE")
            manager = NewDeleteMgr(retryPauseTime=self.step.retryDelay,
                                   numberOfRetries=self.step.retryCount,
                                   **stageOutCall)

        # This is where the deleted files go
        filesDeleted = []

        for fileDict in self.job['input_files']:
            self.logger.debug("Deleting LFN: %s", fileDict.get('lfn'))
            fileForTransfer = {'LFN': fileDict.get('lfn'),
                               'PFN': None,  # PFNs are assigned in the Delete Manager
                               'PNN': None,  # PNN is assigned in the delete manager
                               'StageOutCommand': None}
            if self.deleteOneFile(fileForTransfer, manager, overrides['waitTime']):
                filesDeleted.append(fileForTransfer)

        # Alan: I do not get why we would have two sets of files to be deleted!
        if hasattr(self.step, 'filesToDelete'):
            # files from the configTree to be deleted
            for k, v in viewitems(self.step.filesToDelete.dictionary_()):
                if k.startswith('file'):
                    self.logger.info("Deleting LFN: %s", v)
                    fileForTransfer = {'LFN': v,
                                       'PFN': None,
                                       'PNN': None,
                                       'StageOutCommand': None}
                    if self.deleteOneFile(fileForTransfer, manager, overrides['waitTime']):
                        filesDeleted.append(fileForTransfer)

        if not filesDeleted:
            raise DeleteMgrError(WM_JOB_ERROR_CODES[60313])

        # Now we've got to put things in the report
        for fileDict in filesDeleted:
            self.report.addRemovedCleanupFile(**fileDict)

    def deleteOneFile(self, fileForTransfer, manager, waitTime):
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)

        try:
            manager(fileToDelete=fileForTransfer)
            # Afterwards, the file should have updated info.
            return fileForTransfer

        except Alarm:
            msg = "Indefinite hang during stageOut of logArchive"
            self.logger.error(msg)
        except Exception as ex:
            self.logger.error("General failure in StageOut for DeleteFiles. Error: %s", str(ex))

        signal.alarm(0)

    def post(self, emulator=None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        self.logger.info("Steps.Executors.%s.post called", self.__class__.__name__)
        return None
