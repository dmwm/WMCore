import time

class DataCache(object): 
    _finishedWFs = [] # Global cache holding workflow lists 
    
    @staticmethod
    def getFinishedWorkflows():
        return DataCache._finishedWFs;

    @staticmethod
    def setFinishedWorkflows(workflowLists):
        DataCache._finishedWFs = workflowLists;