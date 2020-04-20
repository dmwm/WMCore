"""
File       : RestApiHub.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: REST API module for MicroService class.
"""

# futures
from __future__ import print_function, division

# cherrypy modules
import cherrypy

# WMCore modules
from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.MicroService.Service.Data import Data


class RestApiHub(RESTApi):
    """
    RestInterface defines REST APIs for MicroService.
    They are mounted to entry point defined in _add method.
    """
    def __init__(self, app, config, mount):
        """
        :arg app: reference to application object; passed to all entities.
        :arg config: reference to configuration; passed to all entities.
        :arg str mount: API URL mount point; passed to all entities."""

        RESTApi.__init__(self, app, config, mount)

        cherrypy.log("MicroService entire configuration:\n%s" % Configuration.getInstance())
        cherrypy.log("MicroService REST configuration subset:\n%s" % config)

        self._add({"status": Data(app, self, config, mount),
                   "info": Data(app, self, config, mount)})
