#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for WMBS Monitoring.
"""

import time
import logging

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.Requests import JSONRequests

class WMBSRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        
        RESTModel.__init__(self, config)

        self.daofactory = DAOFactory(package = "WMCore.WMBS", logger = self,
                                     dbinterface = self.dbi)

        self.addDAO('GET', "listsubtypes", "Monitoring.ListSubTypes")
        self.addDAO('GET', "listjobstates", "Monitoring.ListJobStates")
        self.addDAO('GET', "listjobsbysub", "Monitoring.ListJobsBySub",
                    args = ["subscriptionId"], 
                    validation = [self.subscriptionIDValidate])
        self.addDAO('GET', "listworkflowefficiency", 
                    "Monitoring.ListWorkflowEfficiency",
                    args = ["subscriptionId"], 
                    validation = [self.subscriptionIDValidate])
        self.addDAO('GET', "subscriptionstatus", 
                    "Monitoring.SubscriptionStatus",
                    args = ["subscriptionType"],
                    validation = [self.subTypeValidate])
        self.addDAO('GET', "listrunningjobs", "Monitoring.ListRunningJobs")
        self.addMethod('GET', "listjobstatus", self.listJobStatus,
                    args = ["jobState", "interval"],
                    validation = [self.jobStatusValidate])
        
        self.addDAO('GET', "workflowstatus", "Workflow.Status")
        self.addDAO('GET', "workflowsummary", "Monitoring.WorkflowSummary")
        self.addDAO('GET', "tasksummary", "Monitoring.TaskSummaryByWorkflow",
                    args = ["workflowName"])
        
        self.addDAO('GET', "test", "Workflow.Test")
        
        resourceDAOFactory = DAOFactory(package = "WMCore.ResourceControl", 
                                        logger = self, dbinterface = self.dbi)
        self.addDAO('GET', "listthresholdsforsubmit", "ListThresholdsForSubmit",
                     args = ["tableFormatt"], 
                     validation = [self.setTableFormat],
                     daoFactory = resourceDAOFactory)
        
        self.addDAO('GET', "listthresholdsforcreate", "ListThresholdsForCreate",
                     args = ["tableFormatt"], 
                     validation = [self.setTableFormat],
                     daoFactory = resourceDAOFactory)
        
        dbsDAOFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                  logger = self, dbinterface = self.dbi)
        self.addDAO('GET', "dbsbufferstatus", "Status",
                    daoFactory = dbsDAOFactory)
        return
    
    def jobStatusValidate(self, input):
        """
        _listJobStatus_

        Handler for the listjobstatus method.  This takes two arguments
        from the webserver:
          jobState - The state to display, defaults to success.
          interval - The amount of time to display, defaults to 2 hours.

        This will then query the jobstate view in the jobdump design document
        in the couch server, format the results and then return them.  This
        request is proxied through the WMBS DAS server as webpages served up
        from here can't talk to the couch database directly.
        """
        jobState = input.setdefault("jobState", "running")
        interval = int(input.setdefault("interval", 7200))

        return input
    
    def listJobStatus(self, jobState, interval):
        
        endTime = int(time.time())
        startTime = endTime - int(interval)
        # running state is recorded in wmbs
        # TODO: find the better way to handle this
        if jobState == "running":
            return self.methods["GET"]["listrunningjobs"]["call"]()
        
        # need to fix this
        # other complete states are recorded in couch
        # 
#        if jobState == "all":
#            endKey = 'endkey=%d' % startTime
#            startKey = 'startkey=%d' % endTime
#            order = 'descending=true'
#            base = '/tier1_skimming/_design/jobdump/_view/stateChangesByTime?' 
#            url = "%s&%s&%s&%s" % (base, order, endKey, startKey)            
#        else:
#            endKey = 'endkey=["%s",%d]' % (jobState, startTime)
#            startKey = 'startkey=["%s",%d]' % (jobState, endTime)
#            order = 'descending=true'
#            base = '/tier1_skimming/_design/jobdump/_view/stateChangesByState?' 
#            url = "%s&%s&%s&%s" % (base, order, endKey, startKey)
#
#        myRequester = JSONRequests(url = "cmssrv52:5984")
#        requestResult = myRequester.get(url)[0]
#
#        dasResult = []
#        for result in requestResult["rows"]:
#            if type(result["key"]) == list:
#                dasResult.append({"couch_record": result["id"],
#                                  "timestamp": result["key"][1],
#                                  "state": result["key"][0],
#                                  "job_name": result["value"]})
#            else:
#                dasResult.append({"couch_record": result["id"],
#                                  "timestamp": result["key"],
#                                  "state": result["value"][1],
#                                  "job_name": result["value"][0]})                
#        return dasResult

    def subscriptionIDValidate(self, input):
        input["subscriptionId"] = int(input["subscriptionId"])
        return input
            
    def subTypeValidate(self, input):
        input.setdefault("subscriptionType", "All")
        return input
    
    def setTableFormat(self, input):
        input["tableFormat"] = True
        return input