#!/usr/bin/env python
""" Main Module for announcing requests """
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import logging
import cherrypy
import WMCore.Lexicon
from WMCore.HTTPFrontEnd.RequestManager.BulkOperations import BulkOperations
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities

class Announce(BulkOperations):
    """ Page for Data Ops to announce requests """
    def __init__(self, config):
        BulkOperations.__init__(self, config)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def index(self, all=0):
        """ Page for announcing requests """
        requests = Utilities.requestsWhichCouldLeadTo('announced')
        return self.templatepage("BulkOperations", operation="Announce", 
                                  actions=None, requests=requests, all=all)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def handleAnnounce(self, **kwargs):
        """ Handler for announcing requests """
        requests = self.requestNamesFromCheckboxes(kwargs)
        for requestName in requests:
            WMCore.Lexicon.identifier(requestName)
            ChangeState.changeRequestStatus(requestName, 'announced')
        return self.templatepage("Acknowledge", participle="announced", 
                                 requests=requests)

