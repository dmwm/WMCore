#!/usr/bin/env python
""" Module for changes to groups of requests """
import logging
import threading
import types
import cherrypy
import urllib
from WMCore.WebTools.WebAPI import WebAPI
import WMCore.Lexicon


class BulkOperations(WebAPI):
    """ Base class for pages intended to make changes to groups of requests """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        # Take a guess
        self.templatedir = config.templates
        self.yuiroot = config.yuiroot
        self.searchFields = ["RequestName", "RequestType"]
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def requestNamesFromCheckboxes(self, kwargs):
        """ For use with tables that send hidden parameters "checkbox<requestName>" """
        requests = []
        for key, value in kwargs.iteritems():
            if isinstance(value, basestring):
                kwargs[key] = value.strip()
            if key.startswith("checkbox"):
                requestName = key[8:]
                WMCore.Lexicon.identifier(requestName)
                requests.append(key[8:])
        return requests

    def filteredRequests(self, value, field):
        """ Search for a regular expression in a certain field of all requests """
        result = []
        requests = self.requests()
        for request in requests:
            if request.get(urllib.unquote(field), '').find(urllib.unquote(value)) != -1:
                result.append(request)
        return result

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def search(self, value, field):
        """ Doesn't copy security settings of original page """
        return self.draw(self.filteredRequests(value, field))
