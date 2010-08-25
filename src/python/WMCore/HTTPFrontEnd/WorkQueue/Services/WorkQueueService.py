#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.Wrappers import JsonWrapper
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface

#TODO: needs to point to the global workqueue if it can make it for the both
from WMCore.WorkQueue.WorkQueue import WorkQueue

class WorkQueueService(ServiceInterface):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def register(self):
        
        # we don't populate wmbs - WorkQueueManager does (in the LocalQueue)
        self.model.config.queueParams['PopulateFilesets'] = False
        self.wq = WorkQueue(logger=self.model, dbi=self.model.dbi, **self.model.config.queueParams)

        self.model.addMethod('POST', 'getwork', self.wq.getWork, args=["siteJobs", "pullingQueueUrl"])
        self.model.addMethod('GET', 'status', self.wq.status, args=["status", "before", "after",
                                        "elementIDs", "subs", "dictKey"])
        self.model.addMethod('PUT', 'synchronize', self.wq.synchronize, args=["child_url", "child_report"])
        
        self.model.addMethod('PUT', 'gotwork', self.wq.gotWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'failwork', self.wq.failWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'donework', self.wq.doneWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'cancelwork', self.wq.cancelWork, args=["elementIDs"])
        #TODO: this needs to be more clearly defined (current deleteWork doesn't do anything) 
        #self.model.addMethod('DELETE', 'deletework', self.wq.deleteWork, args=["elementIDs"])
        
    
    #TODO if it needs to be validated, add validation
    #The only requirment of validation function is take input (dict) type return input.
    #raise exception if it fails.       
    #def validateArgs(self, input):
    #    return input
            
