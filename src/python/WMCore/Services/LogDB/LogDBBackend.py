#!/usr/bin/env python
"""
LogDBBackend

Interface to LogDB persistent storage
"""

import time
import datetime

from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
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
        dbs = self.server.listDatabases()
        create = False if db_name in self.server.listDatabases() else True 
        self.db = self.server.connectDatabase(db_name, create=create, size = 10000)
        self.design = kwds.get('design', 'LogDB')
        self.view = kwds.get('view', 'requests')

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
        res = self.db.commitOne(data)
        return res

    def get(self, request, agent, mtype="comment"):
        """Retrieve all entries from LogDB for given request/agent pair"""
        self.check(request, agent)
        spec = {'key':request}
        docs = self.db.loadView(self.design, self.view, spec)
        return docs

    def delete(self, request, agent):
        """Delete entry in LogDB for given request/agent pair"""
        self.check(request, agent)
        pass
