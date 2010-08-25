#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.Wrappers import jsonwrapper
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory

#TODO: needs to point to the global workqueue if it can make it for the both
from WMCore.WorkQueue.WorkQueue import globalQueue

class WorkQueueRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        
        self.wq = globalQueue(logger=self, dbi=self.dbi)
        #only support get for now
        self.methods = {'GET':{}, 'POST':{}, 'PUT':{}, 'DELETE':{}}
        self.addService('POST', 'getwork', self.getWork, args=[])
        self.addService('PUT', 'synchronize', self.synchronize, args=["child_url", "child_report"])
        self.addService('PUT', 'gotwork', self.wq.gotWork, args=["parentElementID"])
        self.addService('PUT', 'failwork', self.wq.failWork, args=["parentElementID"])
        self.addService('PUT', 'successwork', self.wq.successWork, args=["parentElementID"])
        self.addService('DELETE', 'deletework', self.wq.deleteWork, args=["parentElementID"])
        
    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        add service (or any other method handler) in 
        """ 
        #TODO Wrap the function to the dict format (json)
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': [],
                                         'version': version}
        
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
        decodedChildReport = jsonwrapper.loads(child_report)
        
        result = self.wq.synchronize(child_url, decodedChildReport)
        #print result
        return result