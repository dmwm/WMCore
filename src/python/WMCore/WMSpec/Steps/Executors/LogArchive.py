#!/usr/bin/env python
"""
_Step.Executor.LogArchive_

Implementation of an Executor for a LogArchive step

"""

__revision__ = "$Id: LogArchive.py,v 1.1 2009/12/11 16:35:35 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import inspect
import os
import os.path
import logging
import re
import tarfile
import time

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.FwkJobReport.Report             import Report
import WMCore.Storage.StageOutMgr as StageOutMgr

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

        print "Steps.Executors.LogArchive.pre called"
        return None

    def execute(self, emulator = None, **overrides):
        """
        _execute_


        """
        #Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )

        matchFiles = [
            ".log$",
            "^FrameworkJobReport.xml$",
            "^FrameworkJobReport-Backup.xml$",
            "^PSet.py$"
            ]

        #Okay, we need a stageOut Manager
        manager = StageOutMgr.StageOutMgr(**overrides)
        manager.numberOfRetries = self.step.retryCount
        manager.retryPauseTime  = self.step.retryDelay

        #Now we need to find all the reports
        logFilesForTransfer = []
        #Look in the taskSpace first
        logFilesForTransfer.extend(self.findFilesInDirectory(self.stepSpace.taskSpace.location, matchFiles))
        #Now check the step spaces
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                #Don't try to parse yourself, it never works
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logFilesForTransfer.extend(self.findFilesInDirectory(stepLocation, matchFiles))

        #What if it's empty?
        if len(logFilesForTransfer) == 0:
            msg = "Could find no log files in job"
            logging.error(msg)
            return logFilesForTransfer

        #Now that we've gone through all the steps, we have to tar it out
        tarBallLocation = os.path.join(self.stepSpace.location, 'logTarball.tar.gz')
        tarBall         = tarfile.open(tarBallLocation, 'w:gz')
        for file in logFilesForTransfer:
            tarBall.add(file)
        tarBall.close()


        fileInfo = {'LFN': self.getLFN(tarName),
            'PFN' : tarBallLocation,
            'SEName' : None,
            'GUID' : None
            }
        manager(fileInfo)

        print fileInfo
        #Now tag things
        self.step.output.outputPFN = fileInfo['PFN']
        self.step.output.SEName    = fileInfo['SEName']
        self.step.output.LFN       = fileInfo['LFN']
        print self.step.output



        #And now we have to send it


        return



    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )
        
        print "Steps.Executors.StageOut.post called"
        return None


    def findFilesInDirectory(self, dirName, matchFiles):
        """
        _findFilesInDirectory_
        
        Given a directory, it matches the files to the specified patterns
        """


        logFiles = []
        for file in os.listdir(dirName):
            fileName = os.path.join(dirName, file)
            if os.path.isdir(fileName):
                #If directory, use recursion
                logFiles.extend(self.findFilesInDirectory(fileName, matchFiles))
            else:
                for match in matchFiles:
                    #Go through each type of match
                    if re.search(match, file):
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

        runPadding = None
        runNumber  = None

        for file in self.job['input_files']:
            for run in file['runs']:
                runNumber  = int(run.run)
                runPadding = str(runNumber // 1000).zfill(4)

        if not runNumber:
            # no jobNumber - use day and hope for no collisions
            runPadding = time.gmtime()[7] # what day is it?
            runNumber  = self.job['name']


        year, month, day = reqTime[:3]

        #LFN = 'test'

        LFN = "/store/unmerged/logs/prod/%s/%s/%s/%s/%s/%s/%s" % \
              (year, month, day, self.report.data.workload,
               runPadding, runNumber, tarName)


        return LFN
