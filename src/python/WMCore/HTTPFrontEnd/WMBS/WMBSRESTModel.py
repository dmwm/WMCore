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
        self.addDAO('GET', 'jobsbysubs', 'Monitoring.JobsBySubscription')
        self.addDAO('GET', 'jobcountbysubs', 'Monitoring.JobCountBySubscriptionAndRun')
        
        
        
    def _sanitise(self, verb, args, kwargs):
        """
        don't override this in subclass
        Take the arguments and ignore everything apart from the first value in 
        args - this is the method name. We can then use this to pull out 
        configuration from self.methods.
        
        I'm not sure how secure this is. There is presumably a way that you 
        could inject a different dao into the methods dictionary, but I don't 
        know how you would do that without compromising the machine running the 
        server, and even then I don't know how you'd do it without changing code
        and disrupting the service.
        """
        dao = self.daofactory(classname=self.methods[verb][args[0]]['dao'])
        return dao.execute(**kwargs)
        
    def addDAO(self, verb, methodKey, dao, version=1):
        """
        add dao (or any other method handler) in self.methods
        self.method need to be initialize if sub class doesn't want to take provide by
        """
        self.methods[verb][methodKey] = {'dao': dao,
                                         'call': self._sanitise,
                                         'version': version}