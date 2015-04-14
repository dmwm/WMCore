#!/usr/bin/env python
"""
LogDBBackend

Interface to LogDB persistent storage
"""

import time
import datetime

from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError, CouchConflictError
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Services.LogDB.LogDBExceptions import LogDBError

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
    rmap = dict(map="function(doc){ if(doc.request) emit(doc.request, doc)}")
    views = dict(requests=rmap)
    doc = dict(_id="_design/LogDB", views=views)
    return doc


class LogDBBackend(object):
    """
    Represents persistent storage for LogDB
    """
    def __init__(self, db_url, db_name, identifier, agent, **kwds):
        self.db_url = db_url
        self.server = CouchServer(db_url)
        self.db_name = db_name
        self.dbid = identifier
        self.agent = agent
        create = kwds.get('create', False)
        size = kwds.get('size', 10000)
        self.db = self.server.connectDatabase(db_name, create=create, size=size)
        self.design = kwds.get('design', 'LogDB') # name of design document
        self.view = kwds.get('view', 'requests') # name of view to look-up requests
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

    def check(self, request):
        """Check that given request name is valid"""
        # TODO: we may add some logic to check request name, etc.
        if  not request:
            raise LogDBError("Request name is empty")

    def prefix(self, mtype):
        """Generate agent specific prefix for given message type"""
        if  self.agent:
            # we add prefix for agent messages, all others will not have this index
            mtype = 'agent-%s' % mtype
        return mtype

    def post(self, request, msg='', mtype="comment"):
        """Post new entry into LogDB for given request"""
        self.check(request)
        mtype = self.prefix(mtype)
        data = {"request":request, "agent": self.dbid, "ts":tstamp(), "msg":msg, "type":mtype}
        res = self.db.commitOne(data)
        return res

    def get(self, request, mtype=None):
        """Retrieve all entries from LogDB for given request"""
        self.check(request)
        spec = {'request':request}
        if  mtype:
            spec.update({'type':mtype})
        docs = self.db.loadView(self.design, self.view, spec)
        return docs

    def delete(self, request):
        """Delete entry in LogDB for given request"""
        self.check(request)
        mtype = self.prefix(mtype)
        docs = self.get(request, mtype)
        ids = [r['value']['_id'] for r in docs.get('rows', [])]
        res = self.db.bulkDeleteByIds(ids)
        return res

    def summary(self, request):
        """Generate summary document for given request"""
        docs = self.get(request)
        out = [] # output list of documents
        odict = {}
        for doc in docs.get('rows', []):
            entry = doc['value']
            key = (entry['request'], entry['type'])
            if  entry['type'].startswith('agent-'):
                if  key in odict:
                    if  entry['ts'] > odict[key]['ts']:
                        odict[key] = clean_entry(entry)
                else:
                    odict[key] = clean_entry(entry)
            else: # keep all user-based messages
                odict.setdefault(key, []).append(clean_entry(entry))
        for key, val in odict.items():
            doc = {'request':request, 'agent':self.dbid}
            if  isinstance(val, list):
                for item in val:
                    rec = dict(doc)
                    rec.update(item)
                    out.append(rec)
            else:
                doc.update(val)
                out.append(doc)
        return out
