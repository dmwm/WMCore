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
        self.backupPath = "./oldSandboxes"
        self.tempFolder 
        self.sandboxPath = None
        self.config = config

    def loadPKL(pklFile):
        with open(pklFile, 'rb') as file:
            data = pickle.load(file)
        return data

    def savePKL(pklFile, data):
        with open(pklFile, 'wb') as file:
            pickle.dump(data, file)

    def loadJobPKL(self, pklFile):
        if self.data is None:
            self.job = load


    def updateSandbox(self, jobPKL, workload):
        date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
        backupFile = f"{self.backupPath}/{jobPKL['workflow']}_{date}.tar.bz2"

        shutil.copyfile(jobPKL['sandbox'], backupFile)

        tempDir = TemporaryDirectory()
        tFile = tarfile.open(jobPKL['sandbox'], "r")
        tFile.extractall(tempDir)
        
        shutil.copyfile(jobPKL['spec'], tempDir+'/WMSandbox/WMWorkload.pkl')


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
        
        self.updateSandbox()

        return

    def getModifierParam(self, jobType, param, defaultReturn = {}):
        """
        _getAlgoParam_

        Get a parameter from the config for the current algorithm and given
        job type
        """
        modName = self.__class__.__name__
        modArgs = getattr(self.config.RetryManager, modName)

        if hasattr(modArgs, jobType):
            algoParams = getattr(modArgs, jobType)
        else:
            algoParams = modArgs.default

        if hasattr(algoParams, param):
            return getattr(algoParams, param)
        else:
            logging.error("No %s for %s algorithm and %s job type" % (param, modName, jobType))
            return defaultReturn
    
    def modifyJob(self, job):
        """
        Executes the functions to modify the job
        """
        pass
