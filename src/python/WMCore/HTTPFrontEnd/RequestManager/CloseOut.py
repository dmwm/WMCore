#!/usr/bin/env python
""" Main Module for closing out requests """
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import logging
import cherrypy
from WMCore.HTTPFrontEnd.RequestManager.BulkOperations import BulkOperations
import WMCore.Lexicon
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrAuth import ReqMgrAuth

class CloseOut(BulkOperations):
    """ Page for Data Ops to close out requests """
    def __init__(self, config):
        BulkOperations.__init__(self, config)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def index(self, all=0):
        """ Page for closing requests """
        requests = Utilities.requestsWhichCouldLeadTo('closed-out')
        return self.templatepage("BulkOperations", operation="CloseOut", 
                                  actions=None, requests=requests, all=all)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def handleCloseOut(self, **kwargs):
        """ Handler for closing out requests """
        requests = BulkOperations.requestNamesFromCheckboxes(self, kwargs)
        for requestName in requests:
            WMCore.Lexicon.identifier(requestName)
            ChangeState.changeRequestStatus(requestName, 'closed-out')
        return self.templatepage("Acknowledge", participle="closed out", 
                                 requests=requests)

