#!/usr/bin/env python
"""
_Step.Executor.LogCollect_

Implementation of an Executor for a  LogCollect step.
"""
from __future__ import print_function

import datetime
import logging
import os
import signal
import socket
import tarfile

from Utils.IteratorTools import grouper
from WMCore.Algorithms.Alarm import Alarm, alarmHandler
from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.StageInMgr import StageInMgr
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.WMRuntime.Tools.Scram import Scram, getSingleScramArch, isCMSSWSupported
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig


class LogCollect(Executor):
    """
    _LogCollect_

    Execute a LogCollect Step

    """

    def pre(self, emulator=None):
        """
        _pre_

        Pre execution checks

        """
        # Are we using an emulator?
        if emulator is not None:
            return emulator.emulatePre(self.step)

        logging.info("Steps.Executors.%s.pre called", self.__class__.__name__)
        return None

    def execute(self, emulator=None):
        """
        _execute_

        """
        # Are we using emulators again?
        if emulator is not None:
            return emulator.emulate(self.step, self.job)

        logging.info("Steps.Executors.%s.execute called", self.__class__.__name__)

        scramCommand = self.step.application.setup.scramCommand
        cmsswVersion = self.step.application.setup.cmsswVersion
        scramArch = getSingleScramArch(self.step.application.setup.scramArch)
        overrideCatalog = getattr(self.step.application, 'overrideCatalog', None)

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()
            if overrides.get('logRedirectSiteLocalConfig', None):
                siteCfg = loadSiteLocalConfig()
                overrides.update(siteCfg.localStageOut)

        # Set wait to over an hour
        waitTime = overrides.get('waitTime', 3600 + (self.step.retryDelay * self.step.retryCount))

        # hardcode CERN EOS T2_CH_CERN stageout parameters
        eosStageOutParams = {}
        eosStageOutParams['command'] = overrides.get('command', "xrdcp")
        eosStageOutParams['option'] = overrides.get('option', "--wma-disablewriterecovery")
        eosStageOutParams['phedex-node'] = overrides.get('phedex-node', "T2_CH_CERN")
        eosStageOutParams['lfn-prefix'] = overrides.get('lfn-prefix', "root://eoscms.cern.ch//eos/cms")

        try:
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
        tarName = '%s-%s-%s-%i-logs.tar' % (self.report.data.workload, taskName, host, self.job["counter"])
        tarLocation = os.path.join(self.stepSpace.location, tarName)

        # Supported by any release beyond CMSSW_8_X, however DaviX is broken until CMSSW_10_4_X
        # see: https://github.com/cms-sw/cmssw/issues/25292
        useEdmCopyUtil = True
        if isCMSSWSupported(cmsswVersion, "CMSSW_10_4_0"):
            pass
        elif scramArch.startswith('slc7_amd64_'):
            msg = "CMSSW too old or not fully functional to support edmCopyUtil, using CMSSW_10_4_0 instead"
            logging.warning(msg)
            cmsswVersion = "CMSSW_10_4_0"
            scramArch = "slc7_amd64_gcc820"
        elif scramArch.startswith('slc6_amd64_'):
            msg = "CMSSW too old or not fully functional to support edmCopyUtil, using CMSSW_10_4_0 instead"
            logging.warning(msg)
            cmsswVersion = "CMSSW_10_4_0"
            scramArch = "slc6_amd64_gcc700"
        else:
            useEdmCopyUtil = False
        logging.info("Using edmCopyUtil: %s", useEdmCopyUtil)

        # setup Scram needed to run edmCopyUtil
        if useEdmCopyUtil:
            logging.info("Creating software area for %s under %s", cmsswVersion, scramArch)
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

            # specify TFC if necessary
            if overrideCatalog:
                copyCommand += " -c %s" % overrideCatalog

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
                    self.report.addInputFile(sourceName="logArchives", lfn=log['lfn'])
            else:
                logging.error("Unable to copy logArchives to local disk")
                for log in logs:
                    self.report.addSkippedFile(log['lfn'], None)

        # create tarfile if any logArchive copied in
        if localLogs:
            with tarfile.open(tarLocation, 'w:') as tarFile:
                for log in localLogs:
                    path = log.split('/')
                    tarFile.add(name=log,
                                arcname=os.path.join(path[-3], path[-2], path[-1]))
                    os.remove(log)
        else:
            msg = "Unable to copy any logArchives to local disk"
            logging.error(msg)
            raise WMExecutionFailure(60312, "LogCollectError", msg)

        # now staging out the LogCollect tarfile
        logging.info("Staging out LogCollect tarfile to EOS (skipping CASTOR)")
        now = datetime.datetime.now()
        lfn = "/store/logs/prod/%i/%.2i/%s/%s/%s" % (now.year, now.month, "WMAgent",
                                                     self.report.data.workload,
                                                     os.path.basename(tarLocation))

        tarInfo = {'LFN': lfn,
                   'PFN': tarLocation,
                   'PNN': None,
                   'GUID': None}

        # perform mandatory stage out to CERN EOS
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            eosStageOutMgr(tarInfo)
        except Alarm:
            msg = "Indefinite hang during stageOut of LogCollect to EOS"
            logging.error(msg)
            raise WMExecutionFailure(60409, "LogCollectTimeout", msg)
        except Exception as ex:
            msg = "Unable to stageOut LogCollect to Castor:\n"
            msg += str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60408, "LogCollectStageOutError", msg)
        signal.alarm(0)

        # add to job report
        self.report.addOutputFile(outputModule="LogCollect", aFile=tarInfo)
        outputRef = getattr(self.report.data, self.stepName)
        outputRef.output.pfn = tarInfo['PFN']
        outputRef.output.location = tarInfo['PNN']
        outputRef.output.lfn = tarInfo['LFN']

        # we got this far, delete ALL input files assigned to this job
        for log in self.job["input_files"]:
            # give up after timeout of 1 minutes
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(60)
            try:
                fileToDelete = {'LFN': log['lfn'],
                                'PFN': None,
                                'PNN': None,
                                'StageOutCommand': None}
                deleteMgr(fileToDelete=fileToDelete)
            except Alarm:
                logging.error("Indefinite hang during delete of logArchive")
            except Exception as ex:
                logging.error("Unable to delete logArchive: %s", ex)
            signal.alarm(0)

        return

    def post(self, emulator=None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        logging.info("Steps.Executors.%s.post called", self.__class__.__name__)
        return None
