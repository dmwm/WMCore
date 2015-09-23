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

from WMCore.WMSpec.Steps.Executor import Executor

from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.Storage.StageInMgr import StageInMgr
from WMCore.Storage.DeleteMgr import DeleteMgr

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
        # Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )

        print "Steps.Executors.LogCollect.pre called"
        return None

    def execute(self, emulator = None):
        """
        _execute_

        """
        # Are we using emulators again?
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

        stageOutParams = {"command": "srmv2-lcg",
                          "se-name": seName,  "lfn-prefix": lfnPrefix}

        # Set EOS stage out params
        seEOSName    = overrides.get('seName',    "srm-eoscms.cern.ch")
        lfnEOSPrefix = overrides.get('lfnPrefix', "srm://srm-eoscms.cern.ch:8443/srm/v2/server?SFN=/eos/cms/store/logs") 
        
        stageEOSOutParams = {"command": "srmv2-lcg",
                             "se-name": seEOSName,  "lfn-prefix": lfnEOSPrefix}    
        # Okay, we need a stageOut Manager
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
            ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True
        try:
            if not useNewStageOutCode:
                # old style
                stageOutMgr = StageOutMgr(**stageOutParams)
                stageOutEOSMgr = StageOutMgr(**stageEOSOutParams)
                stageInMgr = StageInMgr()
                deleteMgr = DeleteMgr()
            else:
                # new style (is even working ???)
                #logging.info("LOGCOLLECT IS USING NEW STAGEOUT CODE")
                #stageOutMgr = StageOutMgr(retryPauseTime  = self.step.retryDelay,
                #                          numberOfRetries = self.step.retryCount,
                #                          **overrides)
                #stageInMgr = StageInMgr(retryPauseTime  = 0,
                #                        numberOfRetries = 0)
                #deleteMgr = DeleteMgr(retryPauseTime  = 0,
                #                      numberOfRetries = 0)
                stageOutMgr = StageOutMgr(**stageOutParams)
                stageOutEOSMgr = StageOutMgr(**stageEOSOutParams)
                stageInMgr = StageInMgr()
                deleteMgr = DeleteMgr()
        except Exception as ex:
            msg = "Unable to load StageIn/Out/Delete Impl: %s" % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60312, "MgrImplementationError", msg)

        # Now we need the logs
        logs = []
        for log in self.job["input_files"]:
            logs.append({"LFN": log["lfn"]})

        # create output tar file
        taskName = self.report.getTaskName().split('/')[-1]
        host = socket.gethostname().split('.')[0]
        tarName = '%s-%s-%s-%i-logs.tar' % (self.report.data.workload, taskName, host , self.job["counter"])
        tarLocation = os.path.join(self.stepSpace.location, tarName)
        tarFile = tarfile.open(tarLocation, 'w:')

        addedFilesToTar = 0
        for log in logs:

            # stage in logArchive from mass storage
            # give up after timeout of 10 minutes
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(600)
            logArchive = None
            try:
                logArchive = stageInMgr(**log)
                self.report.addInputFile(sourceName = "logArchives", lfn = log['LFN'])
            except Alarm:
                msg = "Indefinite hang during stageIn of LogCollect"
                logging.error(msg)
                self.report.addError(self.stepName, 60407, "LogCollectTimeout", msg)
                self.report.persist("Report.pkl")
            except StageOutFailure as ex:
                msg = "Unable to StageIn %s" % log['LFN']
                logging.error(msg)
                # Don't do anything other then record it
                self.report.addSkippedFile(log.get('PFN', None), log['LFN'])
            except Exception:
                raise
            signal.alarm(0)

            if logArchive:
                addedFilesToTar =+ 1
                path = logArchive['PFN'].split('/')
                tarFile.add(name = logArchive["PFN"],
                            arcname = os.path.join(path[-3],
                                                   path[-2],
                                                   os.path.basename(logArchive['PFN'])))

                # delete logArchive from local disk
                os.remove(logArchive["PFN"])

        tarFile.close()

        if addedFilesToTar == 0:
            # Then we have no output, all the files failed => fail job
            msg = "No logs staged in during LogCollect step"
            logging.error(msg)
            raise WMExecutionFailure(60312, "LogCollectError", msg)

        # we got this far, delete input
        for log in logs:

            # delete logArchive from mass storage
            # give up after timeout of 5 minutes
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(300)
            try:
                fileToDelete = {"LFN": log["lfn"],
                                "PFN": None,
                                "SEName": None,
                                "StageOutCommand": None}
                deleteMgr(fileToDelete = fileToDelete)
            except Alarm:
                msg = "Indefinite hang during delete of LogCollect"
                logging.error(msg)
            except Exception as ex:
                msg = "Unable to delete files:\n"
                msg += str(ex)
                logging.error(msg)
            signal.alarm(0)

        now = datetime.datetime.now()
        if userLogs:
            lfn = "%s/%s/logs/%s" % (lfnBase, self.report.data.workload, os.path.basename(tarLocation))
        else:
            lfn = "/store/logs/prod/%i/%.2i/%s/%s/%s" % (now.year, now.month, "WMAgent",
                                                         self.report.data.workload,
                                                         os.path.basename(tarLocation))

        tarInfo = {'LFN'    : lfn,
                   'PFN'    : tarLocation,
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
        except Exception as ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print "MSG: %s" % msg
            raise WMExecutionFailure(60408, "LogCollectStageOutError", msg)
        signal.alarm(0)

        try:
            # try eos statge out
            stageOutEOSMgr(tarInfo)
        except Exception as ex:
            #When stageOut fails print the message but do nothing.
            msg = "Unable to stage out log archive to EOS:\n"
            msg += str(ex)
            print "MSG: %s" % msg
            
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
        # Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )

        print "Steps.Executors.LogCollect.post called"
        return None
