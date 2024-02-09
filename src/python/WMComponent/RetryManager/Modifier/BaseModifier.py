import pickle, os
import sys
import time
import logging
from WMCore.WMRuntime.SandboxCreator import SandboxCreator


class BaseModifier(object):

    def __init__(self):
        self.backupPath = ""
        self.sandboxPath = None
        self.requiresModify = True

    def isReady(self, job):
        """
        Actual function that does the work

        """
        pass

    def getJobSandbox(self):
        pass

    def updateSandbox(self):
        pass

    def getJob(self, jobList):
        pass


    def getCacheDirectory(self, job):
        """
        returns the cache directory of a job
        """
        cacheDir = (job['cache_dir'])
        return cacheDir
    
    def getJobPkl(self, cacheDir):
        """
        returns the jobPkl file in a job's cache directory 
        """
        jobPkl = '{}/job.pkl'.format(cacheDir)
        return jobPkl
    
    def getWorkload(self):
        """
        _getWorkload_

        
        """
        # Creates copy of original sandbox, unzips it, and retrieves the path to the uncompressed sandbox
        self.sandboxPath=self.getJobSandbox()

        pklPath = self.sandboxPath+'WMSandbox/WMWorkload.pkl'

        configHandle = open(pklPath, "rb")
        workload = pickle.load(configHandle)
        configHandle.close()

        return workload

    def setWorkload(self, workload):
        """
        _setWorkload_

        
        """
        pklPath=self.sandboxPath+'WMSandbox/WMWorkload.pkl'

        #Pkl the modified object
        with open(pklPath, 'wb') as pf:
            pickle.dump(workload, pf)
        
        self.updateSandbox()

        return
    
    def getModifierParam(self, jobType, param, defaultReturn = {}):
        """
        _getModifierParam_

        Get a parameter from the config for the current modifier and given
        failure type
        """
        modifierName = self.__class__.__name__
        modifierArgs = getattr(self.config.RetryManager, modifierName)

        if hasattr(modifierArgs, jobType):
            modifierParams = getattr(modifierArgs, jobType)
        else:
            modifierParams = modifierArgs.default

        if hasattr(modifierParams, param):
            return getattr(modifierParams, param)
        
        else:
            logging.error("No %s for %s algorithm and %s job type" % (param, modifierName, jobType))
            return defaultReturn
