#!/usr/bin/env python
"""
_Step.Executor.LogCollect_

Implementation of an Executor for a StageOut step

"""

__revision__ = "$Id: LogCollect.py,v 1.1 2010/05/05 21:06:07 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import os
import os.path
import logging
import signal
import string
import tarfile
import time

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.FwkJobReport.Report             import Report

import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.StageInMgr  as StageInMgr
import WMCore.Storage.DeleteMgr   as DeleteMgr
from WMCore.Storage.StageOutError import StageOutFailure
        
from WMCore.WMSpec.Steps.Executors.LogArchive import Alarm, alarmHandler

class LogCollect(Executor):
    """
    _LogCollect_

    Execute a LogCollect Step

    """        

    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """

        #Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )


        
        print "Steps.Executors.LogCollect.pre called"
        return None


    def execute(self, emulator = None):
        """
        _execute_


        """
        #Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )


        # We need an lfnBase to continue
        if not hasattr(self.step, 'lfnBase'):
            msg = "No lfnBase attached to step"
            logging.error(msg)
            raise WMExecutionFailure(60312, "NoBaseLFN", msg)


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


        # Now we need THREE managers
        try:
            deleteMgr   = DeleteMgr.DeleteMgr()
            stageInMgr  = StageInMgr.StageInMgr()
            stageOutMgr = StageOutMgr.StageOutMgr(**stageOutCall)
        except StandardError, ex:
            msg = "Unable to load StageIn/Out/Delete Impl: %s" % str(ex)
            raise WMExecutionFailure(60312, "MgrImplementationError", msg)


        # Now we need the logs
        logs = self.report.getInputFilesFromStep(stepName = self.stepName)
        readyFiles = []

        for file in logs:
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(waitTime)
            try:
                file = stageInMgr(**file)
                readyFiles.append(file)
            except Alarm:
                msg = "Indefinite hang during stageIn of LogCollect"
                logging.error(msg)
            except StageOutFailure, ex:
                msg = "Unable to StageIn %s" % file['LFN']
                logging.error(msg)
                # Don't do anything other then record it
                self.report.addSkippedFile(file['PFN'], file['LFN'])
            signal.alarm(0)

        if len(readyFiles) == 0:
            # Then we have no output; all the files failed
            # Panic!
            msg = "No logs staged in during LogCollect step"
            raise WMExecutionFailure(60312, "LogCollectError", msg)

        tarPFN = self.createArchive(readyFiles)

        tarInfo = {'LFN'    : "%s/%s" % (self.lfnBase, os.path.basename(tarPFN)),
                   'PFN'    : tarPFN,
                   'SEName' : None,
                   'GUID'   : None}


        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            stageOutMgr(**tarInfo)
        except Alarm:
                msg = "Indefinite hang during stageOut of LogCollect"
                logging.error(msg)
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            raise WMExecutionFailure(60312, "LogCollectStageOutError", msg)
        signal.alarm(0)

        # If we're still here we didn't die on stageOut
        for f in readyFiles:
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(waitTime)
            try:
                deleteMgr(**f)
            except Alarm:
                msg = "Indefinite hang during delete of LogCollect"
                logging.error(msg)
            except Exception, ex:
                msg = "Unable to delete files:\n"
                msg += str(ex)
                raise WMExecutionFailure(60312, "DeleteError", msg)
            signal.alarm(0)


        # Add to report
        outputRef = getattr(self.report.data, self.stepName)
        outputRef.output.pfn = tarInfo['PFN']
        outputRef.output.location = tarInfo['SEName']
        outputRef.output.lfn = tarInfo['LFN']



        return 



        




    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )
        
        print "Steps.Executors.LogCollect.post called"
        return None





    def createArchive(self, fileList):
        """
        _createArchive_

        Creates a tarball archive for log files
        """

        tarName         = '%s-%i-Logs.tar' % (self.report.data.workload, int(time.time()))
        tarBallLocation = os.path.join(self.stepSpace.location, tarName)
        tarBall         = tarfile.open(tarBallLocation, 'w:')
        for f in fileList:
            tarBall.add(f)
        tarBall.close()

        return tarBallLocation
