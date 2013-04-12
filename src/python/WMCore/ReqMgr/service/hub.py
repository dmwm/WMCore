"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""

import cherrypy

from WMCore.Configuration import Configuration
from WMCore.Database.CMSCouch import CouchServer, Database, Document
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import RawFormat

from WMCore.ReqMgr.service.auxiliary import HelloWorld
from WMCore.ReqMgr.service.auxiliary import Info
from WMCore.ReqMgr.service.request import Request



class DatabasePool(object):
    """
    Object will hold a few attributes being database (CouchDB)
    database connection instances. (Attributes can't be directly set
    on an instance of object.)
    
    """
    pass



class Hub(RESTApi):
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
    
    cherrypy.log("Creating CouchDB connection instances ...")
    # TODO
    # the aim here is to reduce overhead to instantiate CouchDB connection
    # on every single database operation. Should be concentrated at a
    # single place. More connections will be created here (metadata database,
    # wmstats, ...) 
    reqmgr_couchdb = Database(config.couch_reqmgr_db,
                              config.couch_host)
    db_pool = DatabasePool()
    db_pool.reqmgr_couchdb = reqmgr_couchdb
    
    # Makes raw format as default
    #self.formats.insert(0, ('application/raw', RawFormat()))
    self._add({"hello": HelloWorld(self, app, config, mount),
               "about": Info(app, self, config, mount, db_pool),
               "info": Info(app, self, config, mount, db_pool),
               "request": Request(app, self, config, mount, db_pool),
              })