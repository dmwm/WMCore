"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""

import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import RawFormat

from WMCore.reqmgr.service.couchdb import ReqMgrCouchDB
from WMCore.reqmgr.service.auxiliary import HelloWorld
from WMCore.reqmgr.service.auxiliary import Info
from WMCore.reqmgr.service.auxiliary import Group
from WMCore.reqmgr.service.auxiliary import Team
from WMCore.reqmgr.service.auxiliary import Software
from WMCore.reqmgr.service.request import Request




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
    
    db_handler = ReqMgrCouchDB(config) 
    
    # Makes raw format as default
    #self.formats.insert(0, ('application/raw', RawFormat()))
    self._add({"hello": HelloWorld(self, app, config, mount),
               "about": Info(app, self, config, mount, db_handler),
               "info": Info(app, self, config, mount, db_handler),
               "request": Request(app, self, config, mount, db_handler),
               "group": Group(app, self, config, mount, db_handler),
               "team": Team(app, self, config, mount, db_handler),
               "sw": Software(app, self, config, mount, db_handler),
              })