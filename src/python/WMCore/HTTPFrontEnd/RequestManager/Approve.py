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
        self.wmstatWriteURL = "%s/%s" % (config.couchUrl.rstrip('/'), config.wmstatDBName)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def index(self):
        """ Page for approving requests """
        return self.draw(self.requests())

    def requests(self):
        """ Base list of the requests """
        return Utilities.requestsWhichCouldLeadTo('assignment-approved')

    def draw(self, requests):
        return self.templatepage("BulkOperations", operation="Approve",
                                  actions=["Approve", "Reject"],
                                  searchFields = ["RequestName", "RequestType"],
                                  requests=requests)

    @cherrypy.expose
    #@cherrypy.tools.secmodv2() security issue fix
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())    
    def handleApprove(self, **kwargs):
        """ Handler for approving requests """
        requests = self.requestNamesFromCheckboxes(kwargs)
        particple = ''
        for requestName in requests:
            if kwargs['action'] == 'Reject':
                participle = 'rejected'
                ChangeState.changeRequestStatus(requestName, 'rejected', wmstatUrl = self.wmstatWriteURL)
            else:
                participle = 'approved'
                ChangeState.changeRequestStatus(requestName, 'assignment-approved', wmstatUrl = self.wmstatWriteURL)
            priority = kwargs.get(requestName+':priority', '')
            if priority != '':
                Utilities.changePriority(requestName, priority, self.wmstatWriteURL)
        return self.templatepage("Acknowledge", participle=participle,
                                 requests=requests)
