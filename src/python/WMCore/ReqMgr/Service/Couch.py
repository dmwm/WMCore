"""
Wrapper module interfacing calls to CMSCouch.

The aim here is to reduce overhead to instantiate CouchDB connections
on every single database operation. Wrapping database interaction
at a single place should ease error handling, connection re-instantiation
if necessary (to be dealt with later).

The client code profits from error handling done here and eliminating
try-except blocks.

"""

import sys
import traceback
import cherrypy

from WMCore.Database.CMSCouch import CouchServer, Database, Document
from WMCore.Database.CMSCouch import CouchError, CouchNotFoundError


class ReqMgrCouch(object):
    
    def _create_conn(self, db_name):
        cherrypy.log("Creating CouchDB connection to '%s' ... " % db_name)
        return Database(dbname=db_name, url=self.config.couch_host)
    
    
    def get_db(self, db_name):
        """
        Return corresponding instance of CMSCouch.Database already
        created in the constructor.
        
        """
        try:
            db = getattr(self, db_name)
            return db
        except AttributeError:
            trace = traceback.format_exception(*sys.exc_info())
            trace_str = ''.join(trace)
            msg = "Wrong database name '%s':\n%s" % (db_name, trace_str)
            cherrypy.log(msg)
            raise cherrypy.HTTPError(500, "Internal application error.")  
        
        
    def __init__(self, config):
        self.config = config
        cherrypy.log("Creating CouchDB connection instances to "
                     "CouchDB host: '%s' ..." % config.couch_host)
        # each CouchDB database will be available under it name in this instance
        dbs = (config.couch_reqmgr_db,
               config.couch_reqmgr_aux_db)
               # will be necessary later:
               #config.couch_config_cache_db,
               #config.couch_workload_summary_db,
               # WMStats, due to shared database, will not be necessary
               #config.couch_wmstats_db,
        for db in dbs:
            setattr(self, db, self._create_conn(db))
            
            
    def document(self, db_name, id):
        """
        Return a documents id from database db_name.
        
        """
        try:
            db = self.get_db(db_name)
            doc = db.document(id)
            return doc
        except CouchError, ex:
            msg = ("ERROR: Query of CouchDB database '%s', "
                   "document '%s' failed." % (db_name, id))
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(400, msg)
        

    def view(self, db_name, app_name, view_name, options={}):
        """
        Return results of a view query.
        
        """
        try:
            db = self.get_db(db_name)
            request_docs = db.loadView(app_name, view_name, options=options)
            return request_docs
        except CouchError, ex:
            msg = ("ERROR: Query of CouchDB database '%s', "
                   "view '%s' failed." % (db_name, app_name))
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(400, msg)