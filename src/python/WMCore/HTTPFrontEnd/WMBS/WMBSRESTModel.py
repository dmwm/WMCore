#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.MySQL.Jobs.Monitoring import JobsByState

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
        self.methods = {'GET':{'jobs':{'dao': JobsByState(self, self.dbi),
                                       'call': self.sanitise,
                                       'version': 1}}}

    def sanitise(self, verb, args, kwargs):
        """
        Take the arguments and ignore everything apart from the first value in 
        args - this is the method name. We can then use this to pull out 
        configuration from self.methods.
        
        I'm not sure how secure this is. There is presumably a way that you 
        could inject a different dao into the methods dictionary, but I don't 
        know how you would do that without compromising the machine running the 
        server, and even then I don't know how you'd do it without changing code
        and disrupting the service.
        """
        return self.methods[verb][args[0]]['dao'].execute()
        