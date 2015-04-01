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

def formatReply(answer, *items):
    """Take reply from couch bulk api and format labeling errors etc
    """
    result, errors = [], []
    for ans in answer:
        if 'error' in ans:
            errors.append(ans)
            continue
        for item in items:
            if item.id == ans['id']:
                item.rev = ans['rev']
                result.append(item)
                break
    return result, errors

def tstamp():
    "Return timestamp with microseconds"
    now = datetime.datetime.now()
    base = str(time.mktime(now.timetuple())).split('.')[0]
    tstamp = '%s.%s' % (base, now.microsecond)
    return float(tstamp)

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
    def __init__(self, db_url, db_name='logdb', **kwds):
        if  'logger' in kwds:
            self.logger = kwds['logger']
        else:
            import logging
            self.logger = logging

        self.db_url = db_url
        self.server = CouchServer(db_url)
        self.db_name = db_name
        self.dbs = {} # map of agent db server, will be filled dynamically
        self.design = kwds.get('design', 'LogDB') # name of design document
        self.view = kwds.get('view', 'requests') # name of view to look-up requests

    def dbserver(self, agent):
        """Get or construct pointer to agent db server"""
        if  agent not in self.dbs:
            db_name = '%s_%s' % (self.db_name, agent)
            create = False if db_name in self.server.listDatabases() else True
            self.dbs[agent] = self.server.connectDatabase(db_name, create=create, size = 10000)
            uri = '/%s/_design/%s' % (db_name, self.design)
            data = design_doc()
            try:
                # insert design doc, if fails due to conflict continue
                # conflict may happen due to concurrent client connection who
                # created first this doc
                self.dbs[agent].put(uri, data)
            except CouchConflictError:
                pass
        return self.dbs[agent]

    def check(self, request, agent):
        """Check that given request/agent names are valid"""
        # TODO: we may add some logic to check request and agents names
        # e.g. request name can be validated, while agent name should be
        # from known agent list, etc.
        if  not request:
            raise LogDBError("Request name is empty")
        if  not agent:
            raise LogDBError("Agent name is empty")

    def post(self, request, agent, msg, mtype="comment"):
        """Post new entry into LogDB for given request/agent pair"""
        self.check(request, agent)
        data = {"request":request, "ts":tstamp(), "msg":msg, "type":mtype}
        res = self.dbserver(agent).commitOne(data)
        return res

    def get(self, request, agent, mtype="comment"):
        """Retrieve all entries from LogDB for given request/agent pair"""
        self.check(request, agent)
        spec = {'key':request}
        docs = self.dbserver(agent).loadView(self.design, self.view, spec)
        return docs

    def delete(self, request, agent):
        """Delete entry in LogDB for given request/agent pair"""
        self.check(request, agent)
        pass
