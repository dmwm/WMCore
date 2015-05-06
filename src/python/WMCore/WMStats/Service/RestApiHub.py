"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""

import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import JSONFormat

from WMCore.WMStats.Service.WMStats import WMStats


class RestApiHub(RESTApi):
    """
    Server object for REST data access API.
    
    """
    def __init__(self, app, config, mount):
        """
        :arg app: reference to application object; passed to all entities.
        :arg config: reference to configuration; passed to all entities.
        :arg str mount: API URL mount point; passed to all entities."""
        
        RESTApi.__init__(self, app, config, mount)
        
        cherrypy.log("WMStats entire configuration:\n%s" % Configuration.getInstance())    
        cherrypy.log("WMstats REST hub configuration subset:\n%s" % config)
        # only allows json format for return value
        self.formats =  [('application/json', JSONFormat())]
        self._add({"wmstats": WMStats(app, self, config, mount),
                   #"logdb": LogDB(app, self, config, mount)
                  })