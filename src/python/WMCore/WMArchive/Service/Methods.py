#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Methods.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module consists of all REST APIs required by WMArchive service
             Every API is designed as a class with appropriate get/post/put/delete
             methods, see RESTEntity class for more details.
"""
# futures
from __future__ import print_function, division

# system modules
import re
import json
import cherrypy

# WMCore modules
import WMCore
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.REST.Format import JSONFormat

# WMArchive modules
from WMCore.WMArchive.Service.Manager import WMArchiveManager

# global regexp
PAT_EXAMPLE = re.compile(r"^[a-zA-Z]$")

class WMAInfo(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.config = config
        self.config = config
        self.mgr = WMArchiveManager(config)
                
    def validate(self, apiobj, method, api, param, safe):
        "Validate user input"
        if  not param.args or not param.kwargs:
            return False # this class does not need any parameters
        return True

    @restcall
    @tools.expires(secs=-1)
    def get(self):
        wmcore_reqmgr_version = WMCore.__version__
        result = {"WMArchive version": self.mgr.version()}
        return rows([result])

class WMAGet(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.config = config
        self.mgr = WMArchiveManager(config)
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        # here we implement what should be validated
        validate_str("query", param, safe, PAT_EXAMPLE, optional=True)

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, query):
        "GET request with given query, all the work is done by WMArchiveManager"
        docs = self.mgr.read(query)
        return docs

class WMAPost(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.config = config
        self.mgr = WMArchiveManager(config)

    def validate(self, apiobj, method, api, param, safe):
        "Validate user input"
        if  not param.args or not param.kwargs:
            return False # this class does not need any parameters
        return True

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self):
        "POST request with given data, all the work is done by WMArchiveManager"
        data = {}
        try :
            data = json.load(cherrypy.request.body)
            status = self.mgr.write(data)
        except Exception as exp:
            raise cherrypy.HTTPError(str(exp))
