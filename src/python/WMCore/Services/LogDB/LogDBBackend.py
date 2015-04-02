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

class LogDBBackend(object):
    """
    Represents persistent storage for LogDB
    """
    def __init__(self, db_url, db_name='logdb', **kwds):
        if  'logger' in kwds:
            self.logger = kwds['logger']
        else:
            import logging
            self.logger = logging

        self.db_url = db_url
        self.server = CouchServer(db_url)
        self.db_name = db_name
        self.db = self.server.connectDatabase(db_name, create=False, size = 10000)
        self.design = kwds.get('design', 'LogDB') # name of design document
        self.view = kwds.get('view', 'requests') # name of view to look-up requests

    def check(self, request, agent):
        """Check that given request/agent names are valid"""
        # TODO: we may add some logic to check request and agents names
        # e.g. request name can be validated, while agent name should be
        # from known agent list, etc.
        if  not request:
            raise LogDBError("Request name is empty")
        if  not agent:
            raise LogDBError("Agent name is empty")

    def prefix(self, agent, mtype):
        """Generate agent specific prefix for given message type"""
        if  agent not in ['all', 'general', 'client']:
            # we add prefix for agent messages, all others will not have this index
            mtype = 'agent-%s' % mtype
        return mtype

    def post(self, request, agent='all', msg='', mtype="comment"):
        """Post new entry into LogDB for given request/agent pair"""
        self.check(request, agent)
        mtype = self.prefix(agent, mtype)
        data = {"request":request, "agent": agent, "ts":tstamp(), "msg":msg, "type":mtype}
        res = self.db.commitOne(data)
        return res

    def get(self, request, agent='all', mtype="comment"):
        """Retrieve all entries from LogDB for given request/agent pair"""
        self.check(request, agent)
        mtype = self.prefix(agent, mtype)
        spec = {'key':request}
        docs = self.db.loadView(self.design, self.view, spec)
        return docs

    def delete(self, request, agent):
        """Delete entry in LogDB for given request/agent pair"""
        self.check(request, agent)
        docs = self.get(request, agent)
        ids = [r['value']['_id'] for r in docs.get('rows', [])]
        res = self.db.bulkDeleteByIds(ids)
        return res

    def summary(self, request, agent):
        """Generate summary document for given request/agent pair"""
        docs = self.get(request, agent)
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
            doc = {'request':request, 'agent':agent}
            if  isinstance(val, list):
                for item in val:
                    rec = dict(doc)
                    rec.update(item)
                    out.append(rec)
            else:
                doc.update(val)
                out.append(doc)
        return out
