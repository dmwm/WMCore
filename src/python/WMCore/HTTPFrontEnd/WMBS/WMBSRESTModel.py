#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory

class MonitoringWebpage():
    """
    _MonitoringWebpage_

    Container class for the WMBS monitoring webpage.  This will store references
    to the WMBS REST model and use that to render templates into HTML.
    """
    def __init__(self, restModel):
        self.restModel = restModel
        return

    def execute(self, *args, **kwargs):
        """
        _execute_

        Return the correct webpage template given any parameters passed to the
        monitoring REST method.
        """
        # If no subscription type is specified we'll display all subscriptions
        # on the monitoring webpage.
        if not kwargs.has_key("subType"):
            kwargs["subType"] = "All"
            
        return self.restModel.templatepage("WMBS", **kwargs)

class WMBSRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        self.version = "$Revision: 1.8 $"        
        RESTModel.__init__(self, config)
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self,
                                     dbinterface = self.dbi)

        # The following three methods are used by the monitoring webpages.
        self.addDAO("GET", "listsubtypes", "Monitoring.ListSubTypes")
        self.addDAO("GET", "subscriptionstatus",
                    "Monitoring.SubscriptionStatus", ["subscriptionType"])
        self.addDAO("GET", "listjobsbysub", "Monitoring.ListJobsBySub", 
                    ["subscriptionId"], validation = [self.validateArgs])

        wmbsMonitoringPage = MonitoringWebpage(self)
        self.addRESTObject("GET", "monitoring", wmbsMonitoringPage)

        self.addDAO('GET', 'jobs', 'Monitoring.JobsByState')
        self.addDAO('GET', 'jobcount', 'Monitoring.JobCountByState')
        self.addDAO('GET', 'jobcountbysubs', 'Monitoring.JobCountBySubscriptionAndRun', 
                    ['subscription', 'run'],
                    validation=[self.validateArgs])
        return

    def addRESTObject(self, verb, method, restObject, args = [],
                      validation = [], version = 1):
        """
        _addRESTObject_

        Add a REST object for a particular verb and key to the model.  As far as
        this method is concerned, a REST object is any class instance that has
        an execute() method.  The execute method should gather all positional
        and keyword arguments that will be passed in by the calling code.
        """
        if not self.methods.has_key(verb):
            self.methods[verb] = {}

        self.methods[verb][method] = {"args": args,
                                      "call": restObject.execute,
                                      "validation": validation,
                                      "version": version}
        return

    def addDAO(self, verb, methodKey, daoStr, args=[], validation=[], version=1):
        """
        add dao (or any other method handler) in self.methods
        self.method need to be initialize if sub class doesn't want to take provide by
        """
        if not self.methods.has_key(verb):
            self.methods[verb] = {}

        dao = self.daofactory(classname=daoStr) 
                
        self.methods[verb][methodKey] = {'args': args,
                                         'call': dao.execute,
                                         'validation': validation,
                                         'version': version}
        

    def validateArgs(self, input):
        return input
        
        
        
        
        
