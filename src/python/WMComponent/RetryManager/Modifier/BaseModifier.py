#!/usr/bin/env python

"""
_BaseModifier_


"""

import pickle, os
import logging
import tarfile
import sys
import shutil
from tempfile import TemporaryDirectory
import datetime
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
import pickle


class BaseModifier(object):

    def __init__(self, config):
        object.__init__(self)
        self.backupPath = "oldSandboxes/"
        self.sandboxPath = None
        self.config = config

    def loadPKL(self, pklFile):
        with open(pklFile, 'rb') as file:
            data = pickle.load(file)
        return data

    def savePKL(self, pklFile, data):
        with open(pklFile, 'wb') as file:
            pickle.dump(data, file)

    def loadJobPKL(self, pklFile):
        if self.data is None:
            self.job = load


    def updateSandbox(self, jobPKL, workload): # Not using workload?
        date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
        os.makedirs(os.path.dirname(self.backupPath), exist_ok=True)
        backupFile = f"{self.backupPath}/{jobPKL['workflow']}_{date}.tar.bz2"

        shutil.copyfile(jobPKL['sandbox'], backupFile)

        tempDir = TemporaryDirectory()
        tempDirName = tempDir.name

        tFile = tarfile.open(jobPKL['sandbox'], "r")
        tFile.extractall(tempDirName)
        
        shutil.copyfile(jobPKL['spec'], tempDirName+'/WMSandbox/WMWorkload.pkl')

        archivePath = jobPKL['sandbox']
        with tarfile.open(archivePath, "w:bz2") as tar:
            for folder in os.listdir(tempDirName):
                tar.add(f"{tempDirName}/{folder}", arcname=folder)

        tempDir.cleanup()
        return
        

    def getWorkload(self, jobPKL):
        """
        _getWorkload_

        
        """
        pklPath = jobPKL['spec']

        configHandle = open(pklPath, "rb")
        workload = pickle.load(configHandle)
        configHandle.close()

        return workload

    def setWorkload(self, workload, jobPKL):
        """
        _setWorkload_

        
        """
        pklPath = jobPKL['spec']

        #Pkl the modified object
        with open(pklPath, 'wb') as pf:
            pickle.dump(workload, pf)
        
        self.updateSandbox(jobPKL, workload) # Not using workload in this function updateSandbox

        return

    def getModifierParam(self, jobType, param, defaultReturn = {}):
        """
        _getAlgoParam_

        Get a parameter from the config for the current algorithm and given
        job type
        """
        modName = self.__class__.__name__
        modArgs = getattr(self.config.RetryManager, modName)

        if hasattr(modArgs, jobType): #config.RetryManager.MemoryModifier.Processing = modifierParams
            modifierParams = getattr(modArgs, jobType)
        else:
            modifierParams = modArgs.default #config.RetryManager.MemoryModifier.default = modifierParams

        if hasattr(modifierParams, param):
            return getattr(modifierParams, param)
        else:
            logging.error("No %s for %s algorithm and %s job type" % (param, modName, jobType))
            return defaultReturn
    
    def modifyJob(self, job):
        """
        Executes the functions to modify the job
        """
        pass
