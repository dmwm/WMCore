#!/usr/bin/env python
"""
_Step.Executor.LogArchive_

Implementation of an Executor for a LogArchive step
"""

import logging
import os
import os.path
import random
import re
import signal
import tarfile
import time

import WMCore.Storage.FileManager
import WMCore.Storage.StageOutMgr as StageOutMgr
from Utils.FileTools import calculateChecksums
from WMCore.Algorithms.Alarm import Alarm, alarmHandler
from WMCore.WMException import WMException
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure

lfnGroup = lambda j: str(j.get("counter", 0) / 1000).zfill(4)


class LogArchive(Executor):
    """
    _LogArchive_

    Execute a LogArchive Step

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

    def sendLogToEOS(self, overrides, tarBallLocation, useNewStageOutCode):
        """
        :param overrides: dictionary for setting the eos lfn.
        :return: None (copy log archive to eos location.)
        Don'f fail the job if copy fails
        """
        eosStageOutParams = {}
        eosStageOutParams['command'] = "xrdcp"
        eosStageOutParams['option'] = "--wma-disablewriterecovery"
        eosStageOutParams['phedex-node'] = overrides.get('eos-phedex-node', "T2_CH_CERN")
        eosStageOutParams['lfn-prefix'] = overrides.get('eos-lfn-prefix',
                                                        "root://eoscms.cern.ch//eos/cms/store/logs/prod/recent")

        if not eosStageOutParams['lfn-prefix']:
            # if overrides for eos-lfn-prefix is set to None or "", don't copy the log to eos
            logging.info("No 'lfn-prefix' found, not writing logs to CERN EOS recent area.")
            return
        elif not self.failedPreviousStep:
            # then throw a dice!!! We only want 1% of the success logs
            if random.randint(1, 100) != 1:
                logging.info("Success job! Not saving its logs to CERN EOS recent area.")
                return
            else:
                logging.info("Lucky success job! Saving its logs to CERN EOS recent area.")
        else:
            logging.info("Failed job! Saving its logs to CERN EOS recent area")

        numRetries = 0
        retryPauseT = 0
        if not useNewStageOutCode:
            # old style
            eosmanager = StageOutMgr.StageOutMgr(**eosStageOutParams)
            eosmanager.numberOfRetries = numRetries
            eosmanager.retryPauseTime = retryPauseT
        else:
            # new style
            logging.info("LOGARCHIVE IS USING NEW STAGEOUT CODE For EOS Copy")
            eosmanager = WMCore.Storage.FileManager.StageOutMgr(
                    retryPauseTime=retryPauseT,
                    numberOfRetries=numRetries,
                    **eosStageOutParams)

        eosFileInfo = {'LFN': self.getEOSLogLFN(),
                       'PFN': tarBallLocation,
                       'PNN': None,
                       'GUID': None
                       }

        msg = "Writing logs to CERN EOS recent with retries: %s and retry pause: %s"
        logging.info(msg, eosmanager.numberOfRetries, eosmanager.retryPauseTime)
        try:
            eosmanager(eosFileInfo)
            eosServerPrefix = eosStageOutParams['lfn-prefix'].replace("root://eoscms.cern.ch//eos/cms",
                                                                      "https://eoscmsweb.cern.ch/eos/cms")
            self.report.setLogURL(eosServerPrefix + eosFileInfo['LFN'])
            self.saveReport()
        except Alarm:
            msg = "Indefinite hang during stageOut of logArchive to EOS, ignoring it"
            logging.error(msg)
        except Exception as ex:
            logging.exception("EOS copy failed, lfn: %s. Error: %s", eosFileInfo['LFN'], str(ex))

        return

    def execute(self, emulator=None):
        """
        _execute_

        """
        # Are we using emulators again?
        if emulator is not None:
            return emulator.emulate(self.step, self.job)

        logging.info("Steps.Executors.%s.execute called", self.__class__.__name__)

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()
        logging.info("Using the following overrides: %s ", overrides)
        # Find alternate stageout location
        self.altLFN = overrides.get('altLFN', None)
        self.failedPreviousStep = overrides.get('previousCmsRunFailure', False)

        logging.info("Step configuration is: %s", self.step)
        # Wait timeout for stageOut
        waitTime = overrides.get('waitTime', 3600 + (self.step.retryDelay * self.step.retryCount))

        matchFiles = [
            ".log$",  # matches the scram, wmagent and cmsRun logs
            "FrameworkJobReport.xml",
            "Report.pkl",
            "^PSet.py$",
            "^PSet.pkl$",
            "_condor_std*",  # condor wrapper logs at the pilot top level
        ]

        ignoredDirs = ['Utils', 'WMCore', 'WMSandbox']

        # Okay, we need a stageOut Manager
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
                ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True
        if not useNewStageOutCode:
            # old style
            manager = StageOutMgr.StageOutMgr(**overrides)
            manager.numberOfRetries = self.step.retryCount
            manager.retryPauseTime = self.step.retryDelay
        else:
            # new style
            logging.info("LOGARCHIVE IS USING NEW STAGEOUT CODE")
            manager = WMCore.Storage.FileManager.StageOutMgr(retryPauseTime=self.step.retryDelay,
                                                             numberOfRetries=self.step.retryCount,
                                                             **overrides)

        # Now we need to find all the reports
        # The log search follows this structure: ~pilotArea/jobArea/WMTaskSpaceArea/StepsArea
        # Start looking at the pilot scratch area first, such that we find the condor logs
        # Then look at the job area in order to find the wmagentJob log
        # Finally, at the taskspace area to find the cmsRun/FWJR/PSet files
        pilotScratchDir = os.path.join(self.stepSpace.taskSpace.location, '../../')
        logFilesToArchive = self.findFilesInDirectory(pilotScratchDir, matchFiles, ignoredDirs)

        # What if it's empty?
        if len(logFilesToArchive) == 0:
            msg = "Couldn't find any log files in the job"
            logging.error(msg)
            return logFilesToArchive

        # Now that we've gone through all the steps, we have to tar it out
        tarName = 'logArchive.tar.gz'
        tarBallLocation = os.path.join(self.stepSpace.location, tarName)
        with tarfile.open(tarBallLocation, 'w:gz') as tarBall:
            for f in logFilesToArchive:
                tarBall.add(name=f,
                            arcname=f.replace(self.stepSpace.taskSpace.location, '', 1).lstrip('/'))

        fileInfo = {'LFN': self.getLFN(tarName),
                    'PFN': tarBallLocation,
                    'PNN': None,
                    'GUID': None
                    }
        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)

        try:
            manager(fileInfo)
            self.report.addOutputModule(moduleName="logArchive")
            (adler32, cksum) = calculateChecksums(tarBallLocation)
            reportFile = {"lfn": fileInfo["LFN"], "pfn": fileInfo["PFN"],
                          "location": fileInfo["PNN"], "module_label": "logArchive",
                          "events": 0, "size": 0, "merged": False,
                          "checksums": {'adler32': adler32, 'cksum': cksum}}
            self.report.addOutputFile(outputModule="logArchive", aFile=reportFile)
        except Alarm:
            msg = "Indefinite hang during stageOut of logArchive"
            logging.error(msg)
            self.report.addError(self.stepName, 60404, "LogArchiveTimeout", msg)
            self.saveReport()
            raise WMExecutionFailure(60404, "LogArchiveTimeout", msg)
        except WMException as ex:
            self.report.addError(self.stepName, 60307, "LogArchiveFailure", str(ex))
            self.saveReport()
            raise ex
        except Exception as ex:
            self.report.addError(self.stepName, 60405, "LogArchiveFailure", str(ex))
            self.saveReport()
            msg = "Failure in transferring logArchive tarball\n"
            logging.exception(msg)
            raise WMException("LogArchiveFailure", message=str(ex))
        signal.alarm(0)

        signal.alarm(waitTime)
        self.sendLogToEOS(overrides, tarBallLocation, useNewStageOutCode)
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

    def findFilesInDirectory(self, dirName, matchFiles, ignoredDirs):
        """
        _findFilesInDirectory_

        Given a directory - usually the task space directory - it matches
        the files to the specified patterns.
        """
        logFiles = []
        for f in os.listdir(dirName):
            if f in ignoredDirs:
                continue

            fileName = os.path.join(dirName, f)
            if os.path.isdir(fileName):
                # If directory, use recursion
                logFiles.extend(self.findFilesInDirectory(fileName, matchFiles, ignoredDirs))
            else:
                for match in matchFiles:
                    # Go through each type of match
                    if re.search(match, f):
                        logFiles.append(fileName)
                        break

        # Return final files
        return logFiles

    def getLFN(self, tarName):
        """
        getLFN

        LFNs are messy, do the messy stuff here
        """

        if hasattr(self.task.data, 'RequestTime'):
            reqTime = time.gmtime(int(self.task.data.RequestTime))
        else:
            reqTime = time.gmtime()

        year, month, day = reqTime[:3]

        LFN = "%s/logs/prod/%s/%s/%s%s/%s/%i/%s-%i-%s" % \
              (self.task.taskLogBaseLFN(), year, month, day, self.task.getPathName(),
               lfnGroup(self.job), self.job.get('retry_count', 0), self.job["name"],
               self.job.get('retry_count', 0),
               tarName)

        if self.altLFN:
            LFN = "%s/%s/%s/%i/%s-%i-%s" % (self.altLFN, self.task.getPathName(),
                                            lfnGroup(self.job), self.job.get('retry_count', 0), self.job["name"],
                                            self.job.get('retry_count', 0), tarName)

        return LFN

    def getEOSLogLFN(self):
        """
        getEOSLogLFN

        LFNs are messy, do the messy stuff here
         /eos/cms/store/logs/prod/recent/vlimant_task_TOP-RunIIFall17wmLHEGS-00073__v1_T_180219_102942_9978/TOP-RunIIFall17DRPremix-00053_0/vocms0251.cern.ch-3803-0.log.tar.gz
        """
        taskPath = self.task.getPathName().split("/")
        requestName = taskPath[1]
        lastTask = taskPath[-1]
        LFN = "/%s/%s/%s-%s-%s-log.tar.gz" % (requestName, lastTask, self.job.get("agentName", 'NA'),
                                              self.job["id"], self.job.get('retry_count', 0))

        return LFN
