#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
web server.
"""
# futures
from __future__ import print_function, division

# system modules
import os
import sys
import time
import json

# cherrypy modules
from cherrypy import tools
from cherrypy import config as cherryconf

# WMCore modules
from WMCore.REST.Main import RESTMain

class WMArchiveService(object):
    """
    WMArchive web service class
    """
    def __init__(self, app, config, mount):
        # Update CherryPy configuration
        mime_types  = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True,
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })

        # initialize rest API
        statedir = '/tmp'
        app = RESTMain(config, statedir) # REST application
        mount = '/rest' # mount point for cherrypy service
        api = RestApiHub(app, config.mgr, mount)
