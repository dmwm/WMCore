from builtins import object
import cherrypy
from WMCore.Database.CMSCouch import Database, CouchError
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache

def couch_request_error(func):
    def wrapperFunc(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except CouchError as ex:
            #msg = ("ERROR: Query of CouchDB database %s,  couchapp %s view %s failed."
            #        % (db_name, app_name, view_name))
            #cherrypy.log("%s Reason: %s" % (msg, ex))
            msg = "test"
            raise cherrypy.HTTPError(400, "%s Reason: %s" % (msg, ex))
        return wrapperFunc

def couch_err_handler(decorator):
    def decorate(cls):
        for attr in cls.__dict__:
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate

@couch_err_handler(couch_request_error)
class RESTBackendCouchDB(Database):
    pass


class ReqMgrCouch(object):

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
               #config.couch_wmstats_db,
        for db in dbs:
            setattr(self, db, self._create_conn(db))

        aux_db = self.get_db(config.couch_reqmgr_aux_db)
        ReqMgrConfigDataCache.set_aux_db(aux_db)

    def _create_conn(self, db_name):
        cherrypy.log("Creating CouchDB connection to '%s' ... " % db_name)
        return RESTBackendCouchDB(dbname=db_name, url=self.config.couch_host)


    def get_db(self, db_name):
        """
        Return corresponding instance of CMSCouch.Database already
        created in the constructor.

        """
        try:
            db = getattr(self, db_name)
            return db
        except AttributeError:
            import traceback
            import sys
            trace = traceback.format_exception(*sys.exc_info())
            trace_str = ''.join(trace)
            msg = "Wrong database name '%s':\n%s" % (db_name, trace_str)
            cherrypy.log(msg)
            raise cherrypy.HTTPError(500, "Internal application error.")



