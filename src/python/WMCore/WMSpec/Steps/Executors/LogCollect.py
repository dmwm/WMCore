#!/usr/bin/env python
"""
_Step.Executor.LogCollect_

Implementation of an Executor for a StageOut step.
"""

import os
import logging
import signal
import tarfile
import datetime
import socket

from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.FwkJobReport.Report             import Report

import WMCore.Storage.FileManager
import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.StageInMgr  as StageInMgr
import WMCore.Storage.DeleteMgr   as DeleteMgr
from WMCore.Storage.StageOutError import StageOutFailure

from WMCore.Algorithms.Alarm import Alarm, alarmHandler

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

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Set wait to over an hour
        waitTime  = overrides.get('waitTime', 3600 + (self.step.retryDelay * self.step.retryCount))
        seName    = overrides.get('seName',    "srm-cms.cern.ch")
        lfnPrefix = overrides.get('lfnPrefix', "srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms")
        lfnBase   = overrides.get('lfnBase',   "/store/user/jsmith")
        userLogs  = overrides.get('userLogs',  False)
        dontStage = overrides.get('dontStage', False)

        stageOutParams = {"command": "srmv2-lcg",
                          "se-name": seName,  "lfn-prefix": lfnPrefix}
        
        #Okay, we need a stageOut Manager
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
            (overrides.has_key('newStageOut') and overrides.get('newStageOut')):
            useNewStageOutCode = True
        try:
            if not useNewStageOutCode:
                # old style
                deleteMgr   = DeleteMgr.DeleteMgr()
                stageInMgr  = StageInMgr.StageInMgr()
                stageOutMgr = StageOutMgr.StageOutMgr(**stageOutParams)
            else:
                # new style
                logging.info("LOGCOLLECT IS USING NEW STAGEOUT CODE")
                stageOutMgr = WMCore.Storage.FileManager.StageOutMgr(
                                    retryPauseTime  = self.step.retryDelay,
                                    numberOfRetries = self.step.retryCount,
                                    **overrides)
                stageInMgr = WMCore.Storage.FileManager.StageInMgr(
                                    retryPauseTime  = self.step.retryDelay,
                                    numberOfRetries = self.step.retryCount )
                deleteMgr = WMCore.Storage.FileManager.DeleteMgr(
                                    retryPauseTime  = self.step.retryDelay,
                                    numberOfRetries = self.step.retryCount )
    
                deleteMgr   = DeleteMgr.DeleteMgr()
                stageInMgr  = StageInMgr.StageInMgr()
                stageOutMgr = StageOutMgr.StageOutMgr(**stageOutParams)
        except StandardError, ex:
            msg = "Unable to load StageIn/Out/Delete Impl: %s" % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60312, "MgrImplementationError", msg)


        # Now we need the logs
        if not dontStage: # Don't stage or delete files
            logs = []
            for file in self.job["input_files"]:
                logs.append({"LFN": file["lfn"]})

            readyFiles = []
            for file in logs:
                signal.signal(signal.SIGALRM, alarmHandler)
                signal.alarm(waitTime)
                try:
                    output = stageInMgr(**file)
                    readyFiles.append(output)
                    self.report.addInputFile(sourceName = "logArchives",
                                            lfn = file['LFN'])
                except Alarm:
                    msg = "Indefinite hang during stageIn of LogCollect"
                    logging.error(msg)
                    self.report.addError(self.stepName, 60407, "LogCollectTimeout", msg)
                    self.report.persist("Report.pkl")
                except StageOutFailure, ex:
                    msg = "Unable to StageIn %s" % file['LFN']
                    logging.error(msg)
                    # Don't do anything other then record it
                    self.report.addSkippedFile(file.get('PFN', None), file['LFN'])
                except Exception, ex:
                    raise

                signal.alarm(0)

            if len(readyFiles) == 0:
                # Then we have no output; all the files failed
                # Panic!
                msg = "No logs staged in during LogCollect step"
                logging.error(msg)
                raise WMExecutionFailure(60312, "LogCollectError", msg)

            now = datetime.datetime.now()
            tarPFN = self.createArchive(readyFiles)
            if userLogs:
                lfn = "%s/%s/logs/%s" % (lfnBase, self.report.data.workload, os.path.basename(tarPFN))
            else:
                lfn = "/store/logs/prod/%i/%.2i/%s/%s/%s" % (now.year, now.month, "WMAgent",
                                                            self.report.data.workload,
                                                            os.path.basename(tarPFN))

            tarInfo = {'LFN'    : lfn,
                    'PFN'    : tarPFN,
                    'SEName' : None,
                    'GUID'   : None}

            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(waitTime)
            try:
                stageOutMgr(tarInfo)
                self.report.addOutputFile(outputModule = "LogCollect", file = tarInfo)
            except Alarm:
                msg = "Indefinite hang during stageOut of LogCollect"
                logging.error(msg)
                raise WMExecutionFailure(60409, "LogCollectTimeout", msg)
            except Exception, ex:
                msg = "Unable to stage out log archive:\n"
                msg += str(ex)
                print "MSG: %s" % msg
                raise WMExecutionFailure(60408, "LogCollectStageOutError", msg)
            signal.alarm(0)

            # If we're still here we didn't die on stageOut
            for file in self.job["input_files"]:
                signal.signal(signal.SIGALRM, alarmHandler)
                signal.alarm(waitTime)
                try:
                    fileToDelete = {"LFN": file["lfn"],
                                    "PFN": None,
                                    "SEName": None,
                                    "StageOutCommand": None}
                    deleteMgr(fileToDelete = fileToDelete)
                except Alarm:
                    msg = "Indefinite hang during delete of LogCollect"
                    logging.error(msg)
                    raise WMExecutionFailure(60411, "DeleteTimeout", msg)
                except Exception, ex:
                    msg = "Unable to delete files:\n"
                    msg += str(ex)
                    logging.error(msg)
                    raise WMExecutionFailure(60410, "DeleteError", msg)
                signal.alarm(0)


        # Add to report
        outputRef = getattr(self.report.data, self.stepName)
        if dontStage:
            outputRef.output.pfn = 'NotStaged'
            outputRef.output.location = 'NotStaged'
            outputRef.output.lfn = 'NotStaged'
        else:
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
        taskName = self.report.getTaskName().split('/')[-1]
        host = socket.gethostname().split('.')[0]
        tarName         = '%s-%s-%s-%i-logs.tar' % (self.report.data.workload, taskName, host , self.job["counter"])
        tarBallLocation = os.path.join(self.stepSpace.location, tarName)
        tarBall         = tarfile.open(tarBallLocation, 'w:')
        for f in fileList:
            path = f['PFN'].split('/')
            tarBall.add(name = f["PFN"],
                        arcname = os.path.join(path[-3],
                                               path[-2],
                                               os.path.basename(f['PFN'])))
        tarBall.close()

        return tarBallLocation
