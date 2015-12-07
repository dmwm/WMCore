'''
Created on May 19, 2015

'''

from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask

class StatusChangeTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.moveToArchived, 'duration': config.checkStatusDuration}]

    def moveToArchived(self, config):
        """
        gather active data statistics
        """
        
        testbedWMStats = WMStatsReader(config.wmstats_url, reqdbURL=config.reqmgrdb_url)
        reqdbWriter = RequestDBWriter(config.reqmgrdb_url)
        
         
        
        statusTransition = {"aborted": ["aborted-completed", "aborted-archived"],
                            "rejected": ["rejected-archived"]}
        
        for status, nextStatusList in statusTransition.items():
            
            requests = testbedWMStats.getRequestByStatus([status], 
                                                jobInfoFlag = True, legacyFormat = True)
            
            self.logger.info("checking %s workflows: %d" % (status, len(requests)))
            
            if len(requests) > 0:
            
                requestCollection = RequestInfoCollection(requests)
                
                requestsDict = requestCollection.getData()
                numOfArchived = 0
                
                for requestName, requestInfo in requestsDict.items():
                    
                    if requestInfo.getJobSummary().getTotalJobs() == 0:
                        for nextStatus in nextStatusList: 
                            reqdbWriter.updateRequestStatus(requestName, nextStatus)
                        numOfArchived += 1
                
                self.logger.info("Total %s-archieved: %d" % (status, numOfArchived))  
                  
        return
