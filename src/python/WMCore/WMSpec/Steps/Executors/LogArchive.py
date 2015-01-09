#!/usr/bin/env python
"""
_Step.Executor.LogArchive_

Implementation of an Executor for a LogArchive step
"""

import os
import os.path
import logging
import re
import tarfile
import time
import signal
import traceback

from WMCore.WMException import WMException

from WMCore.Algorithms.Alarm import Alarm, alarmHandler

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure

from WMCore.Algorithms.BasicAlgos import calculateChecksums

import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.FileManager


lfnGroup = lambda j : str(j.get("counter", 0) / 1000).zfill(4)

class LogArchive(Executor):
    """
    _LogArchive_

    Execute a LogArchive Step

    """

    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks
        """

        #Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )

        logging.info("Steps.Executors.LogArchive.pre called")
        return None

    def execute(self, emulator = None, **overrides):
        """
        _execute_


        """
        # Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Find alternate stageout location
        self.altLFN = overrides.get('altLFN', None)

        logging.info("Beginning Steps.Executors.LogArchive.Execute")
        logging.info("Using the following overrides: %s " % overrides)
        logging.info("Step is: %s" % self.step)
        # Wait timout for stageOut
        waitTime = overrides.get('waitTime', 3600 + (self.step.retryDelay * self.step.retryCount))

        matchFiles = [
            ".log$",
            "FrameworkJobReport",
            "Report.pkl",
            "Report.pcl",
            "^PSet.py$",
            "^PSet.pkl$"
            ]

        #Okay, we need a stageOut Manager
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
            ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True
        if not useNewStageOutCode:
            # old style
            manager = StageOutMgr.StageOutMgr(**overrides)
            manager.numberOfRetries = self.step.retryCount
            manager.retryPauseTime  = self.step.retryDelay
        else:
            # new style
            logging.info("LOGARCHIVE IS USING NEW STAGEOUT CODE")
            manager = WMCore.Storage.FileManager.StageOutMgr(
                                retryPauseTime  = self.step.retryDelay,
                                numberOfRetries = self.step.retryCount,
                                **overrides)

        #Now we need to find all the reports
        logFilesForTransfer = []
        #Look in the taskSpace first
        logFilesForTransfer.extend(self.findFilesInDirectory(self.stepSpace.taskSpace.location, matchFiles))

        #What if it's empty?
        if len(logFilesForTransfer) == 0:
            msg = "Could find no log files in job"
            logging.error(msg)
            return logFilesForTransfer

        #Now that we've gone through all the steps, we have to tar it out
        tarName         = 'logArchive.tar.gz'
        tarBallLocation = os.path.join(self.stepSpace.location, tarName)
        tarBall         = tarfile.open(tarBallLocation, 'w:gz')
        for f in logFilesForTransfer:
            tarBall.add(name  = f,
                        arcname = f.replace(self.stepSpace.taskSpace.location, '', 1).lstrip('/'))
        tarBall.close()


        fileInfo = {'LFN': self.getLFN(tarName),
            'PFN' : tarBallLocation,
            'SEName' : None,
            'PNN' : None,
            'GUID' : None
            }

        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            manager(fileInfo)
            self.report.addOutputModule(moduleName = "logArchive")
            (adler32, cksum) = calculateChecksums(tarBallLocation)
            reportFile = {"lfn": fileInfo["LFN"], "pfn": fileInfo["PFN"],
#                          "location": fileInfo["SEName"], "module_label": "logArchive",
                          "location": fileInfo["PNN"], "module_label": "logArchive",
                          "events": 0, "size": 0, "merged": False,
                          "checksums": {'adler32': adler32, 'cksum' : cksum}}
            self.report.addOutputFile(outputModule = "logArchive", file = reportFile)
        except Alarm:
            msg = "Indefinite hang during stageOut of logArchive"
            logging.error(msg)
            self.report.addError(self.stepName, 60404, "LogArchiveTimeout", msg)
            self.report.persist("Report.pkl")
            raise WMExecutionFailure(60404, "LogArchiveTimeout", msg)
        except WMException as ex:
            self.report.addError(self.stepName, 60307, "LogArchiveFailure", str(ex))
            self.report.setStepStatus(self.stepName, 0)
            self.report.persist("Report.pkl")
            raise ex
        except Exception as ex:
            self.report.addError(self.stepName, 60405, "LogArchiveFailure", str(ex))
            self.report.setStepStatus(self.stepName, 0)
            self.report.persist("Report.pkl")
            msg = "Failure in transferring logArchive tarball\n"
            msg += str(ex) + "\n"
            msg += traceback.format_exc()
            logging.error(msg)
            raise WMException("LogArchiveFailure", message = str(ex))

        signal.alarm(0)
        return



    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )

        logging.info("Steps.Executors.StageOut.post called")
        return None


    def findFilesInDirectory(self, dirName, matchFiles):
        """
        _findFilesInDirectory_

        Given a directory, it matches the files to the specified patterns
        """


        logFiles = []
        for f in os.listdir(dirName):
            fileName = os.path.join(dirName, f)
            if os.path.isdir(fileName):
                #If directory, use recursion
                logFiles.extend(self.findFilesInDirectory(fileName, matchFiles))
            else:
                for match in matchFiles:
                    #Go through each type of match
                    if re.search(match, f):
                        logFiles.append(fileName)
                        break

        #Return final files
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
