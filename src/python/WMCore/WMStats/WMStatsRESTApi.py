"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""

import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import RawFormat

from WMCore.WMStats.Requests import Requests

class WMStatsRestApi(RESTApi):
    """
    Server object for REST data access API.
      
    """
    def __init__(self):
        RESTApi.__init__(self, app, config, mount)

        cherrypy.log("ReqMgr entire configuration:\n%s" % Configuration.getInstance())    
        cherrypy.log("ReqMgr REST hub configuration subset:\n%s" % config)
        
        db_handler = ReqMgrCouch(config) 
        
        # Makes raw format as default
        #self.formats.insert(0, ('application/raw', RawFormat()))
        self._add({"requests": Requests(self, app, config, mount)
                  })