#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory

#TODO: needs to point to the global workqueue if it can make it for the both
from WMCore.WorkQueue.WorkQueue import WorkQueue

class WorkQueueRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        
        wq = WorkQueue(type='global')
        #only support get for now
        #self.methods = {d'GET':{}}
        self.addService('GET', 'getwork', wq.getWork)
        self.addService('PUT', 'gotwork', wq.gotWork)
        self.addService('PUT', 'failwork', wq.failWork)
        self.addService('PUT', 'successwork', wq.successWork)
        self.addService('DELETE', 'deletework', wq.deleteWork)
        
    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        add service (or any other method handler) in 
        """ 
        #TODO Wrap the function to the dict format (json)
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': [],
                                         'version': version}
        

        
        
        
        
        
        