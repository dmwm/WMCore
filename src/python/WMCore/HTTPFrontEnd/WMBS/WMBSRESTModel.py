#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory

class WMBSRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self,
                                     dbinterface = self.dbi)
        
        #only support get for now
        self.methods = {'GET':{}}
        self.addDAO('GET', 'jobs', 'Monitoring.JobsByState')
        self.addDAO('GET', 'jobcount', 'Monitoring.JobCountByState')
        self.addDAO('GET', 'jobsbysubs', 'Monitoring.JobsBySubscription', ['subscription'])
        self.addDAO('GET', 'jobcountbysubs', 'Monitoring.JobCountBySubscriptionAndRun', ['subscription', 'run'])
        
    def addDAO(self, verb, methodKey, daoStr, args=[], validation=[], version=1):
        """
        add dao (or any other method handler) in self.methods
        self.method need to be initialize if sub class doesn't want to take provide by
        """
        
        
        dao = self.daofactory(classname=daoStr) 
                
        self.methods[verb][methodKey] = {'args': args,
                                         'call': dao.execute,
                                         'validation': [],
                                         'version': version}
        

        
        
        
        
        
        