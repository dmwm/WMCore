#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

import time

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.Requests import JSONRequests

class MonitoringWebpage:
    """
    _MonitoringWebpage_

    Container class for the WMBS monitoring webpages.  This will store
    references to the WMBS REST model and use that to render templates into
    HTML.
    """
    def __init__(self, restModel):
        self.restModel = restModel
        return

    def wmbsStatus(self, *args, **kwargs):
        """
        _wmbsStatus_

        Render the main monitoring page that displays the status for all
        subscriptions in WMBS.  The page itself takes a single parameter from
        the webserver:
          subType - The type of subscription to display.  This will default to
                    All.

        The template itself will take the subscription type and the WMBS
        instance name from the config.
        """
        subType = kwargs.get("subType", "All")

        return self.restModel.templatepage("WMBS", subType = subType,
                                           instance = self.restModel.config.instance)        

    def subscriptionStatus(self, *args, **kwargs):
        """
        _subscriptionStatus_

        Render the subscription status page.  The page itself takes a single
        mandatory parameter from the webserver:
          subscriptionId - The id of the subscription to display. 
        """
        subscriptionId = int(kwargs["subscriptionId"])
        
        return self.restModel.templatepage("WMBSSubscription",
                                           subscriptionId = subscriptionId)

    def jobStatus(self, *args, **kwargs):
        """
        _jobStatus_

        Render the job status page.  The page itself takes two parameters
        from the webserver:
          jobState - What state is displayed
          interval - The amount of time to display

        The defaults will be the success state and 2 hours.  The template itself
        takes the jobState interval, the wmbs instance name and a URL used to
        display the content of couch documents for jobs.
        """
        jobState = kwargs.get("jobState", "success")
        interval = int(kwargs.get("interval", 7200))

        return self.restModel.templatepage("WMBSJobStatus", jobState = jobState,
                                           interval = interval,
                                           instance = self.restModel.config.instance,
                                           couchURL = self.restModel.config.couchURL)
    
    def execute(self, *args, **kwargs):
        """
        _execute_

        Return the correct webpage template given any parameters passed to the
        monitoring REST method.  The following pages are currently supported:
          subscription - Displays details for a particular subscription
          jobstatus - Display status information for jobs in this wmbs instance

        If neither of these pages is specified the default subscription status
        page will be displayed.
        """
        if len(args) >= 1 and args[0] == "subscription":
            return self.subscriptionStatus(*args, **kwargs)
        elif len(args) >= 1 and args[0] == "jobstatus":
            return self.jobStatus(*args, **kwargs)        

        return self.wmbsStatus(*args, **kwargs)

class WMBSRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        self.version = "$Revision: 1.9 $"        
        RESTModel.__init__(self, config)

        self.daos = {}
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = self,
                                     dbinterface = self.dbi)

        self.registerMethods()
        wmbsMonitoringPage = MonitoringWebpage(self)
        self.addRESTObject("GET", "monitoring", wmbsMonitoringPage)

        return

    def getDAO(self, className):
        """
        _getDAO_

        Retrieve a DAO from the DAO cache.  If it does not exist, create it.
        """
        if className not in self.daos.keys():
            self.daos[className] = self.daoFactory(classname = className)

        return self.daos[className]

    def registerMethods(self):
        """
        _registerMethods_

        Register all the actual data methods with the base class.
        """
        self.methods["GET"]["listsubtypes"] = {"args": [],
                                               "call": self.listSubTypes,
                                               "validation": [],
                                               "version": self.version}
        self.methods["GET"]["listjobstates"] = {"args": [],
                                               "call": self.listJobStates,
                                               "validation": [],
                                               "version": self.version}        
        self.methods["GET"]["listjobsbysub"] = {"args": ["subscriptionId"],
                                                "call": self.listJobsBySub,
                                                "validation": [],
                                                "version": self.version}
        self.methods["GET"]["listworkflowefficiency"] = {"args": ["subscriptionId"],
                                                         "call": self.listWorkflowEfficiency,
                                                         "validation": [],
                                                         "version": self.version}
        self.methods["GET"]["listjobstatechanges"] = {"args": ["jobState", "startTime"],
                                                      "call": self.listJobStateChanges,
                                                      "validation": [],
                                                      "version": self.version}
        self.methods["GET"]["subscriptionstatus"] = {"args": ["subType"],
                                                     "call": self.subscriptionStatus,
                                                     "validation": [],
                                                     "version": self.version}
        return

    def listSubTypes(self, *args, **kwargs):
        """
        _listSubTypes_

        Handler for the listsubtypes method.  This will query the wmbs_sub_type
        table and return all the defined subscription types.  It does not take
        any parameters.
        """
        dao = self.getDAO("Monitoring.ListSubTypes")
        return dao.execute()

    def listJobStates(self, *args, **kwargs):
        """
        _listJobStates_

        Handler for the listjobstates method.  This does not take any
        parameters.
        """
        dao = self.getDAO("Monitoring.ListJobStates")
        return dao.execute()    

    def listJobStateChanges(self, *args, **kwargs):
        """
        _listJobStateChanges_

        Handler for the listjobstatechanges method.  This takes two arguments
        from the webserver:
          jobState - The state to display, defaults to success.
          interval - The amount of time to display, defaults to 2 hours.

        This will then query the jobstate view in the jobdump design document
        in the couch server, format the results and then return them.  This
        request is proxied through the WMBS DAS server as webpages served up
        from here can't talk to the couch database directly.
        """
        jobState = kwargs.get("jobState", "success")
        interval = int(kwargs.get("interval", 7200))

        endTime = int(time.time())
        startTime = endTime - interval

        endKey = 'endkey=["%s",%d]' % (jobState, startTime)
        startKey = 'startKey=["%s",%d]' % (jobState, endTime)
        base = '/tier1_skimming/_design/jobdump/_view/jobstate?descending=true&' 
        url = "%s&%s&%s" % (base, endKey, startKey)

        myRequester = JSONRequests(url = "cmssrv52:5984")
        requestResult = myRequester.get(url)[0]

        dasResult = []
        for result in requestResult["rows"]:
            dasResult.append({"couch_record": result["id"],
                              "timestamp": result["key"][1],
                              "state": result["key"][0],
                              "job_name": result["value"]})
            
        return dasResult

    def listJobsBySub(self, *args, **kwargs):
        """
        _listJobsBySub_

        Handler for the listjobsbysub method.  This takes one mandatory
        parameter from the webserver:
          subscriptionId - The subscription id to display jobs for.
        """
        assert "subscriptionId" in kwargs.keys()
        subscriptionId = int(kwargs["subscriptionId"])

        dao = self.getDAO("Monitoring.ListJobsBySub")
        return dao.execute(subscriptionId)

    def listWorkflowEfficiency(self, *args, **kwargs):
        """
        _listWorkflowEfficiency_

        Handler for the listworkflowefficiency method.  This takes one mandatory
        parameter from the webserver:
          subscriptionId - The subscription id to use for the efficiency query.
        """
        assert "subscriptionId" in kwargs.keys()
        subscriptionId = int(kwargs["subscriptionId"])

        dao = self.getDAO("Monitoring.ListWorkflowEfficiency")
        return dao.execute(subscriptionId)    

    def subscriptionStatus(self, *args, **kwargs):
        """
        _subscriptionStatus_

        Handler for the subscriptionstatus method.  This take one optional
        parameter from the webserver:
          subType - The subscription type to query.  This defaults to All.
        """
        subType = kwargs.get("subType", "All")

        dao = self.getDAO("Monitoring.SubscriptionStatus")
        return dao.execute(subType)

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
