"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""

import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import RawFormat

from WMCore.ReqMgr.Service.Couch import ReqMgrCouch
from WMCore.ReqMgr.Service.Auxiliary import HelloWorld
from WMCore.ReqMgr.Service.Auxiliary import Info
from WMCore.ReqMgr.Service.Auxiliary import Group
from WMCore.ReqMgr.Service.Auxiliary import Team
from WMCore.ReqMgr.Service.Auxiliary import Software
from WMCore.ReqMgr.Service.Request import Request
from WMCore.ReqMgr.Service.Request import RequestStatus
from WMCore.ReqMgr.Service.Request import RequestType



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
        
        cherrypy.log("ReqMgr entire configuration:\n%s" % Configuration.getInstance())    
        cherrypy.log("ReqMgr REST hub configuration subset:\n%s" % config)
        
        self.db_handler = ReqMgrCouch(config) 
        db_handler = ReqMgrCouch(config)
        # Makes raw format as default
        #self.formats.insert(0, ('application/raw', RawFormat()))
        self._add({"hello": HelloWorld(self, app, config, mount),
                   "about": Info(app, self, config, mount, db_handler),
                   "info": Info(app, self, config, mount, db_handler),
                   "request": Request(app, self, config, mount, db_handler),
                   "group": Group(app, self, config, mount, db_handler),
                   "team": Team(app, self, config, mount, db_handler),
                   "software": Software(app, self, config, mount, db_handler),
                   "status": RequestStatus(app, self, config, mount),
                   "type": RequestType(app, self, config, mount),
                  })