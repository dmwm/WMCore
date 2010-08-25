#!/usr/bin/env python
"""
_Step.Executor.LogArchive_

Implementation of an Executor for a LogArchive step

"""

__revision__ = "$Id: LogArchive.py,v 1.3 2010/03/03 19:38:07 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

import os
import os.path
import logging
import re
import tarfile
import time
import signal

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
import WMCore.Storage.StageOutMgr as StageOutMgr


class Alarm(Exception):
    pass

def alarmHandler(signum, frame):
    raise Alarm


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

        #Wait fifteen minutes for stageOut
        waitTime = overrides.get('waitTime', 900)

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
            tarBall.add(f)
        tarBall.close()


        fileInfo = {'LFN': self.getLFN(tarName),
            'PFN' : tarBallLocation,
            'SEName' : None,
            'GUID' : None
            }

        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            manager(fileInfo)
        except Alarm:
            msg = "Indefinite hang during stageOut of logArchive"
            logging.error(msg)
        signal.alarm(0)

        #print fileInfo
        #Now tag things
        self.step.output.outputPFN = fileInfo['PFN']
        self.step.output.SEName    = fileInfo['SEName']
        self.step.output.LFN       = fileInfo['LFN']
        #print self.step.output



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

        runPadding = None
        runNumber  = None

        for f in self.job['input_files']:
            for run in f['runs']:
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
