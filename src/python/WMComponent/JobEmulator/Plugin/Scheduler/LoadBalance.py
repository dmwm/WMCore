
import threading

from WMCore.WMFactory import WMFactory

class LoadBalance:

    def __init__(self):
        self.factory = WMFactory('factory','')
        myThread = threading.currentThread()
        self.queries = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.Sites')


    def allocateJob(self):
        #TODO: this is a simpler version than the old job emulator.
        return self.queries.selecNodeWithLeastJobs()
        
