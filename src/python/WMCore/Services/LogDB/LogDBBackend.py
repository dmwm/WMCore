#!/usr/bin/env python
"""
LogDBBackend

Interface to LogDB persistent storage
"""

# syste modules
import time
import datetime
import hashlib

# WMCore modules
from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError, CouchConflictError
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Services.LogDB.LogDBExceptions import LogDBError

# define full list of supported LogDB types
LOGDB_MSG_TYPES = ['info', 'error', 'warning', 'comment']

def gen_hash(key):
    "Generate hash for given key"
    if  not isinstance(key, basestring):
        raise NotImplementedError
    keyhash = hashlib.md5()
    keyhash.update(key)
    return keyhash.hexdigest()

def tstamp():
    "Return timestamp with microseconds"
    now = datetime.datetime.now()
    base = str(time.mktime(now.timetuple())).split('.')[0]
    tstamp = '%s.%s' % (base, now.microsecond)
    return float(tstamp)

def clean_entry(doc):
    """Clean document from CouchDB attributes"""
    for attr in ['_rev', '_id']:
        if  attr in doc:
            del doc[attr]
    return doc

def design_doc():
    """Return basic design document"""
    rmap = dict(map="function(doc){ if(doc.request) emit(doc.request, null)}",
            reduce="_count")
    tmap = dict(map="function(doc){ if(doc.comments) for(i=0;i<doc.comments.length;i++) emit(doc.comments[i].ts, null)}")
    views = dict(requests=rmap, tstamp=tmap)
    doc = dict(_id="_design/LogDB", views=views)
    return doc

class LogDBBackend(object):
    """
    Represents persistent storage for LogDB
    """
    def __init__(self, db_url, db_name, identifier, thread_name, **kwds):
        self.db_url = db_url
        self.server = CouchServer(db_url)
        self.db_name = db_name
        self.dbid = identifier
        self.thread_name = thread_name
        self.agent = kwds.get('agent', 0)
        create = kwds.get('create', False)
        size = kwds.get('size', 10000)
        self.db = self.server.connectDatabase(db_name, create=create, size=size)
        self.design = kwds.get('design', 'LogDB') # name of design document
        self.view = kwds.get('view', 'requests') # name of view to look-up requests
        self.tsview = kwds.get('tsview', 'tstamp') # name of tsview to look-up requests
        if  create:
            uri = '/%s/_design/%s' % (db_name, self.design)
            data = design_doc()
            try:
                # insert design doc, if fails due to conflict continue
                # conflict may happen due to concurrent client connection who
                # created first this doc
                self.db.put(uri, data)
            except CouchConflictError:
                pass

    def deleteDatabase(self):
        """Delete back-end database"""
        if  self.db_name in self.server.listDatabases():
            self.server.deleteDatabase(self.db_name)

    def check(self, request, mtype=None):
        """Check that given request name is valid"""
        # TODO: we may add some logic to check request name, etc.
        if  not request:
            raise LogDBError("Request name is empty")
        if  mtype and mtype not in LOGDB_MSG_TYPES:
            raise LogDBError("Unsupported message type: '%s', supported types %s" \
                    % (mtype, LOGDB_MSG_TYPES))

    def docid(self, request, mtype):
        """Generate doc id, we use double dash to avoid dashes from thread names"""
        return gen_hash('--'.join((request, self.dbid, self.thread_name, mtype)))

    def prefix(self, mtype):
        """Generate agent specific prefix for given message type"""
        if  self.agent:
            # we add prefix for agent messages, all others will not have this index
            mtype = 'agent-%s' % mtype
        return mtype

    def agent_update(self, request, msg='', mtype="info"):
        """Update agent info in LogDB for given request"""
        self.check(request, mtype)
        mtype = self.prefix(mtype)
        rec = {"ts":tstamp(), "msg":msg}
        doc = {"_id": self.docid(request, mtype), "comments": [rec],
                "request":request, "identifier":self.dbid,
                "thr":self.thread_name, "type":mtype}
        try:
            exist_doc = self.db.document(doc["_id"])
            doc["_rev"] = exist_doc["_rev"]
        except CouchNotFoundError:
            # this means document is not exist so we will just insert
            pass
        finally:
            res = self.db.commitOne(doc)
        return res

    def user_update(self, request, msg, mtype='comment'):
        """Update user info in LogDB for given request"""
        rec = {"ts":tstamp(), "msg":msg}
        doc = {"_id": self.docid(request, mtype), "comments": [rec],
                "request":request, "identifier":self.dbid,
                "thr":self.thread_name, "type":mtype}
        try:
            exist_doc = self.db.document(doc["_id"])
            doc["_rev"] = exist_doc["_rev"]
            doc["comments"] += exist_doc["comments"]
        except CouchNotFoundError:
            # this means document is not exist so we will just insert
            pass
        finally:
            res = self.db.commitOne(doc)
        return res

    def get(self, request, mtype=None, detail=True):
        """Retrieve all entries from LogDB for given request"""
        self.check(request, mtype)
        spec = {'request':request, 'reduce':False}
        if  mtype:
            spec.update({'type':mtype})
        if detail:
            spec.update({'include_docs': True})
        docs = self.db.loadView(self.design, self.view, spec)
        return docs

    def get_all_requests(self):
        """Retrieve all entries from LogDB"""
        spec = {'reduce':True, 'group_level':1}
        docs = self.db.loadView(self.design, self.view, spec)
        return docs

    def delete(self, request):
        """Delete entry in LogDB for given request"""
        self.check(request)
        docs = self.get(request, detail=False)
        ids = [r['id'] for r in docs.get('rows', [])]
        res = self.db.bulkDeleteByIDs(ids)
        return res

    def cleanup(self, thr):
        """
        Clean-up docs older then given threshold (thr should be specified in seconds).
        This is done via tstamp view end endkey, e.g.
        curl "http://127.0.0.1:5984/logdb/_design/LogDB/_view/tstamp?endkey=1427912282"
        """
        tstamp = round(time.time()-thr)
        docs = self.db.allDocs() # may need another view to look-up old docs
        spec = {'endkey':tstamp, 'reduce':False}
        docs = self.db.loadView(self.design, self.tsview, spec)
        ids = [d['id'] for d in docs.get('rows', [])]
        self.db.bulkDeleteByIDs(ids)
