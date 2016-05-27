#!/usr/bin/env python
"""
_Step.Executor.LogCollect_

Implementation of an Executor for a LogCollect step.
"""
from __future__ import print_function

import os
import re
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

from WMCore.WMRuntime.Tools.Scram import Scram

from Utils.IterTools import grouper


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

        print("Steps.Executors.LogCollect.pre called")
        return None

    def execute(self, emulator = None):
        """
        _execute_

        """
        scramCommand = self.step.application.setup.scramCommand
        scramArch = self.step.application.setup.scramArch
        cmsswVersion = self.step.application.setup.cmsswVersion

        # Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Set wait to over an hour
        waitTime = overrides.get('waitTime', 3600 + (self.step.retryDelay * self.step.retryCount))

        # Pull out StageOutMgr Overrides
        # switch between old stageOut behavior and new, fancy stage out behavior
        useNewStageOutCode = False
        if 'newStageOut' in overrides and overrides.get('newStageOut'):
            useNewStageOutCode = True

        # hardcode CERN Castor T0_CH_CERN_MSS stageout parameters
        castorStageOutParams = {}
        castorStageOutParams['command'] = overrides.get('command', "srmv2-lcg")
        castorStageOutParams['option'] = overrides.get('option', "")
        castorStageOutParams['se-name'] = overrides.get('se-name', "srm-cms.cern.ch")
        castorStageOutParams['phedex-node'] = overrides.get('phedex-node', "T2_CH_CERN")
        castorStageOutParams['lfn-prefix'] = overrides.get('lfn-prefix', "srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms")

        # hardcode CERN EOS T2_CH_CERN stageout parameters
        eosStageOutParams = {}
        eosStageOutParams['command'] = overrides.get('command', "srmv2-lcg")
        eosStageOutParams['option'] = overrides.get('option', "")
        eosStageOutParams['se-name'] = overrides.get('se-name', "srm-eoscms.cern.ch")
        eosStageOutParams['phedex-node'] = overrides.get('phedex-node', "T2_CH_CERN")
        eosStageOutParams['lfn-prefix'] = overrides.get('lfn-prefix', "srm://srm-eoscms.cern.ch:8443/srm/v2/server?SFN=/eos/cms")

        # are we using the new stageout method ?
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
            ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True

        try:
            if useNewStageOutCode:
                # is this even working ???
                #logging.info("LOGCOLLECT IS USING NEW STAGEOUT CODE")
                #stageOutMgr = StageOutMgr(retryPauseTime  = self.step.retryDelay,
                #                          numberOfRetries = self.step.retryCount,
                #                          **overrides)
                #stageInMgr = StageInMgr(retryPauseTime  = 0,
                #                        numberOfRetries = 0)
                #deleteMgr = DeleteMgr(retryPauseTime  = 0,
                #                      numberOfRetries = 0)
                castorStageOutMgr = StageOutMgr(**castorStageOutParams)
                eosStageOutMgr = StageOutMgr(**eosStageOutParams)
                stageInMgr = StageInMgr()
                deleteMgr = DeleteMgr()
            else:
                castorStageOutMgr = StageOutMgr(**castorStageOutParams)
                eosStageOutMgr = StageOutMgr(**eosStageOutParams)
                stageInMgr = StageInMgr()
                deleteMgr = DeleteMgr()
        except Exception as ex:
            msg = "Unable to load StageOut/Delete Impl: %s" % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60312, "MgrImplementationError", msg)

        # prepare output tar file
        taskName = self.report.getTaskName().split('/')[-1]
        host = socket.gethostname().split('.')[0]
        tarName = '%s-%s-%s-%i-logs.tar' % (self.report.data.workload, taskName, host , self.job["counter"])
        tarLocation = os.path.join(self.stepSpace.location, tarName)

        # check if the cmsswVersion supports edmCopyUtil (min CMSSW_8_X)
        result = re.match("CMSSW_([0-9]+)_([0-9]+)_([0-9]+).*", cmsswVersion)
        useEdmCopyUtil = False
        if result:
            try:
                if int(result.group(1)) >= 8:
                    useEdmCopyUtil = True
            except ValueError:
                pass

        # setup Scram needed to run edmCopyUtil
        if useEdmCopyUtil:
            scram = Scram(
                command=scramCommand,
                version=cmsswVersion,
                initialise=self.step.application.setup.softwareEnvironment,
                directory=self.step.builder.workingDir,
                architecture=scramArch,
                )
            logging.info("Running scram")
            try:
                projectOutcome = scram.project()
            except Exception as ex:
                msg = "Exception raised while running scram.\n"
                msg += str(ex)
                logging.critical("Error running SCRAM")
                logging.critical(msg)
                raise WMExecutionFailure(50513, "ScramSetupFailure", msg)

            if projectOutcome > 0:
                msg = scram.diagnostic()
                logging.critical("Error running SCRAM")
                logging.critical(msg)
                raise WMExecutionFailure(50513, "ScramSetupFailure", msg)
            runtimeOutcome = scram.runtime()
            if runtimeOutcome > 0:
                msg = scram.diagnostic()
                logging.critical("Error running SCRAM")
                logging.critical(msg)
                raise WMExecutionFailure(50513, "ScramSetupFailure", msg)

        # iterate through input files
        localLogs = []
        deleteLogArchives = []
        if useEdmCopyUtil:
            numberOfFilesPerCopy = 10
        else:
            numberOfFilesPerCopy = 1
        for logs in grouper(self.job["input_files"], numberOfFilesPerCopy):

            copyCommand = "env X509_USER_PROXY=%s edmCopyUtil" % os.environ.get('X509_USER_PROXY', None)
            for log in logs:
                copyCommand += " %s" % log['lfn']
            copyCommand += " %s" % self.step.builder.workingDir

            # give up after timeout of 1 minute per input file
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(60 * numberOfFilesPerCopy)

            filesCopied = False
            try:
                if useEdmCopyUtil:
                    logging.info("Running edmCopyUtil")
                    retval = scram(copyCommand)
                    if retval == 0:
                        filesCopied = True
                else:
                    logging.info("Running stageIn")
                    for log in logs:
                        fileInfo = {"LFN": log['lfn']}
                        logArchive = stageInMgr(**fileInfo)
                        if logArchive:
                            filesCopied = True
            except Alarm:
                logging.error("Indefinite hang during edmCopyUtil/stageIn of logArchives")
            except StageOutFailure:
                logging.error("Unable to stageIn logArchives")
            except Exception:
                raise

            signal.alarm(0)

            if filesCopied:
                for log in logs:
                    localLogs.append(os.path.join(self.step.builder.workingDir, os.path.basename(log['lfn'])))
                    deleteLogArchives.append(log)
                    self.report.addInputFile(sourceName = "logArchives", lfn = log['lfn'])
            else:
                logging.error("Unable to copy logArchives to local disk")
                if useEdmCopyUtil:
                    with open('scramOutput.log', 'r') as f:
                        logging.error("Scram output: %s", f.read())
                for log in logs:
                    self.report.addSkippedFile(log['lfn'], None)

        # create tarfile if any logArchive copied in
        if localLogs:
            tarFile = tarfile.open(tarLocation, 'w:')
            for log in localLogs:
                path = log.split('/')
                tarFile.add(name = log,
                            arcname = os.path.join(path[-3],
                                                   path[-2],
                                                   path[-1]))
                os.remove(log)
            tarFile.close()
        else:
            msg = "Unable to copy any logArchives to local disk"
            logging.error(msg)
            raise WMExecutionFailure(60312, "LogCollectError", msg)


        # now staging out the LogCollect tarfile
        logging.info("Staging out LogCollect tarfile to Castor and EOS")
        now = datetime.datetime.now()
        lfn = "/store/logs/prod/%i/%.2i/%s/%s/%s" % (now.year, now.month, "WMAgent",
                                                     self.report.data.workload,
                                                     os.path.basename(tarLocation))

        tarInfo = {'LFN'    : lfn,
                   'PFN'    : tarLocation,
                   'SEName' : None,
                   'PNN'    : None,
                   'GUID'   : None}

        # perform mandatory stage out to CERN Castor
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            castorStageOutMgr(tarInfo)
        except Alarm:
            msg = "Indefinite hang during stageOut of LogCollect to Castor"
            logging.error(msg)
            raise WMExecutionFailure(60409, "LogCollectTimeout", msg)
        except Exception as ex:
            msg = "Unable to stageOut LogCollect to Castor:\n"
            msg += str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60408, "LogCollectStageOutError", msg)
        signal.alarm(0)

        # add to job report
        self.report.addOutputFile(outputModule = "LogCollect", file = tarInfo)
        outputRef = getattr(self.report.data, self.stepName)
        outputRef.output.pfn = tarInfo['PFN']
        outputRef.output.location = tarInfo['PNN']
        outputRef.output.lfn = tarInfo['LFN']


        tarInfo = {'LFN'    : lfn,
                   'PFN'    : tarLocation,
                   'SEName' : None,
                   'PNN'    : None,
                   'GUID'   : None}

        # then, perform best effort stage out to CERN EOS
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            eosStageOutMgr(tarInfo)
        except Alarm:
            logging.error("Indefinite hang during stageOut of LogCollect to EOS")
        except Exception as ex:
            logging.error("Unable to stageOut LogCollect to EOS:\n", ex)
        signal.alarm(0)

        # we got this far, delete input
        for log in deleteLogArchives:

            # give up after timeout of 1 minutes
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(60)
            try:
                fileToDelete = {'LFN': log['lfn'],
                                'PFN': None,
                                'SEName': None,
                                'PNN': None,
                                'StageOutCommand': None}
                deleteMgr(fileToDelete = fileToDelete)
            except Alarm:
                logging.error("Indefinite hang during delete of logArchive")
            except Exception as ex:
                logging.error("Unable to delete logArchive: %s", ex)
            signal.alarm(0)

        return

    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )

        print("Steps.Executors.LogCollect.post called")
        return None
