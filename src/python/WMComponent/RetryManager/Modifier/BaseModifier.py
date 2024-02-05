import pickle, os
import sys
import time
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
import pickle


class BaseModifier(object):

    def __init__(self):
        self.backupPath = ""
        self.sandboxPath = None

    def getJobSandbox(self):
        pass

    def updateSandbox(self):
        pass

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