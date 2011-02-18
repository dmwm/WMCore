#!/usr/bin/env python
""" Main Module for approving requests """
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import requestsWhichCouldLeadTo
import types
import logging
import threading
import cherrypy
import WMCore.Lexicon
from WMCore.WebTools.WebAPI import WebAPI
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import changePriority


class Approve(WebAPI):
    """ Page for Physics group leaders to approve requests """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        # Take a guess
        self.templatedir = config.templates
        self.yuiroot = config.yuiroot
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    @cherrypy.expose
    def index(self, all=0):
        """ Page for approving requests """
        requests = requestsWhichCouldLeadTo('assignment-approved')
        return self.templatepage("Approve", requests=requests, all=all)

    @cherrypy.expose
    def handleApprovalPage(self, **kwargs):
        """ Handler for approving requests """
        # handle the checkboxes
        requests = []
        for key, value in kwargs.iteritems():
            if isinstance(value, types.StringTypes):
                kwargs[key] = value.strip()
            if key.startswith("checkbox"):
                requestName = key[8:]
                WMCore.Lexicon.identifier(requestName)
                requests.append(key[8:])
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
                changePriority(requestName, priority)
        return self.templatepage("Acknowledge", participle=participle, 
                                 requests=requests)

