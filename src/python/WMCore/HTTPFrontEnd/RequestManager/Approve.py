#!/usr/bin/env python
""" Main Module for approving requests """
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import cherrypy
from WMCore.HTTPFrontEnd.RequestManager.BulkOperations import BulkOperations
import WMCore.Lexicon
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities


class Approve(BulkOperations):
    """ Page for Physics group leaders to approve requests """
    def __init__(self, config):
        BulkOperations.__init__(self, config)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def index(self, all=0):
        """ Page for approving requests """
        requests = Utilities.requestsWhichCouldLeadTo('assignment-approved')
        return self.templatepage("BulkOperations", operation="Approve", 
                                  actions=["Approve", "Reject"], 
                                  requests=requests, all=all)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def handleApprove(self, **kwargs):
        """ Handler for approving requests """
        requests = self.requestNamesFromCheckboxes(kwargs)
        particple = ''
        for requestName in requests:
            if kwargs['action'] == 'Reject':
                participle = 'rejected'
                ChangeState.changeRequestStatus(requestName, 'rejected')
            else:
                participle = 'approved'
                ChangeState.changeRequestStatus(requestName, 'assignment-approved')
            priority = kwargs.get(requestName+':priority', '')
            if priority != '':
                Utilities.changePriority(requestName, priority)
        return self.templatepage("Acknowledge", participle=participle, 
                                 requests=requests)

