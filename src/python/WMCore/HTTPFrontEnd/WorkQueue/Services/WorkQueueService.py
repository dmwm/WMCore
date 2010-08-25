#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.Wrappers import JsonWrapper
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface

#TODO: needs to point to the global workqueue if it can make it for the both
from WMCore.WorkQueue.WorkQueue import globalQueue

class WorkQueueService(ServiceInterface):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def register(self):
        
        self.wq = globalQueue(logger=self.model, dbi=self.model.dbi, **self.model.config.queueParams)
        #only support get for now
        self.model.addMethod('POST', 'getwork', self.getWork, args=[])
        self.model.addMethod('POST', 'status', self.status, args=["status", "before", "after", 
                                        "elementIDs", "subs", "dictKey"])
        self.model.addMethod('PUT', 'synchronize', self.synchronize, args=["child_url", "child_report"])
        self.model.addMethod('PUT', 'gotwork', self.gotWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'failwork', self.failWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'donework', self.doneWork, args=["elementIDs"])
        self.model.addMethod('PUT', 'cancelwork', self.cancelWork, args=["elementIDs"])
        #self.model.addMethod('DELETE', 'deletework', self.deleteWork, args=["elementIDs"])
        
            
    def validateArgs(self, input):
        return input
            
    def getWork(self, **kwargs):    
        """
        _getWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        pqUrl = kwargs.pop("PullingQueueUrl", None)
        result = self.wq.getWork(kwargs, pqUrl)
        return result
    
    def synchronize(self, child_url, child_report):
        """
        _synchronize_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedChildReport = JsonWrapper.loads(child_report)
        
        result = self.wq.synchronize(child_url, decodedChildReport)
        #print result
        return result
    
    def doneWork(self, elementIDs):
        """
        _doneWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedElementIDs = JsonWrapper.loads(elementIDs)
        result = self.wq.doneWork(decodedElementIDs)
        #print result
        return result
    
    def failWork(self, elementIDs):
        """
        _failWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedElementIDs = JsonWrapper.loads(elementIDs)
        result = self.wq.doneWork(decodedElementIDs)
        #print result
        return result
    
    def cancelWork(self, elementIDs):
        """
        _cancelWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedElementIDs = JsonWrapper.loads(elementIDs)
        result = self.wq.doneWork(decodedElementIDs)
        #print result
        return result
    
    def gotWork(self, elementIDs):
        """
        _doneWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedElementIDs = JsonWrapper.loads(elementIDs)
        result = self.wq.doneWork(decodedElementIDs)
        #print result
        return result
    
    def deleteWork(self, elementIDs):
        """
        _deleteWork_
        
        TODO: not the best way to handle parameters which is not in dict format
        find the better way to handle it 
        """
        decodedElementIDs = JsonWrapper.loads(elementIDs)
        result = self.wq.doneWork(decodedElementIDs)
        #print result
        return result
    
    def status(self, status = None, before = None, after = None, elementIDs=None, 
               dictKey = None):
        
        if elementIDs != None:
            elementIDs = JsonWrapper.loads(elementIDs)
        
        if before != None:
            before = int(before)
        if after != None:
            after = int(after)
        
        result = self.wq.status(status, before, after, elementIDs, 
                                dictKey)
        
        return result