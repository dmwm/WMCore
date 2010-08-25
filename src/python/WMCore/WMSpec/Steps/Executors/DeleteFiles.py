#!/usr/bin/env python

"""
_Step.Executor.DeleteFiles_

Implementation of an Executor for a Delete step

"""

__revision__ = "$Id: DeleteFiles.py,v 1.3 2010/05/11 16:01:59 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

import os.path
import logging
import signal

from WMCore.WMSpec.Steps.Executor           import Executor

import WMCore.Storage.DeleteMgr as DeleteMgr
        
from WMCore.WMSpec.Steps.Executors.LogArchive import Alarm, alarmHandler

class DeleteFiles(Executor):
    """
    A step run to clean up the unmerged files in a job

    """


    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """

        # Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )


        
        print "Steps.Executors.DeleteFiles.pre called"
        return None



    def execute(self, emulator = None):
        """
        _execute_

        """

        # Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )



        # Look!  I can steal from StageOut
        # DeleteMgr uses the same manager structure as StageOutMgr

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Set wait to 15 minutes
        waitTime = overrides.get('waitTime', 900)

        # Pull out StageOutMgr Overrides
        stageOutCall = {}
        if overrides.has_key("command") and overrides.has_key("option") \
               and overrides.has_key("se-name") and overrides.has_key("lfn-prefix"):
            stageOutCall['command']    = overrides.get('command')
            stageOutCall['option']     = overrides.get('option')
            stageOutCall['se-name']    = overrides.get('se-name')
            stageOutCall['lfn-prefix'] = overrides.get('lfn-prefix')

        # naw man, this is real
        # iterate over all the incoming files
        manager = DeleteMgr.DeleteMgr(**stageOutCall)
        manager.numberOfRetries = self.step.retryCount
        manager.retryPauseTime  = self.step.retryDelay


        # This is where the deleted files go
        filesDeleted = []

        for file in self.job['input_files']:
            fileForTransfer = {'LFN': file.get('lfn'),
                               'PFN': None,  # PFNs are assigned in the Delete Manager
                               'SEName' : None,  # SEName is assigned in the delete manager
                               'StageOutCommand': None}

            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(waitTime)

            try:
                manager(fileToDelete = fileForTransfer)
                #Afterwards, the file should have updated info.
                filesDeleted.append(fileForTransfer)

            except Alarm:
                msg = "Indefinite hang during stageOut of logArchive"
                logging.error(msg)
            except:
                self.report.addError(self.stepName, 1, "StageOutFailure", str(ex))
                self.report.setStepStatus(self.stepName, 1)
                self.report.persist("Report.pkl")
                raise

            signal.alarm(0)


        # Now we've got to put things in the report
        for file in filesDeleted:
            self.report.addRemovedCleanupFile(**file)

                
        return


    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )
        
        print "Steps.Executors.DeleteFiles.post called"
        return None
