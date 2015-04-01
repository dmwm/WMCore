#!/usr/bin/env python
"""
LogDB provides functionality to post/search messages into LogDB.
https://github.com/dmwm/WMCore/issues/5705
"""

# standard modules
import os

# project modules
from WMCore.Services.LogDB.LogDBBackend import LogDBBackend

class LogDB(object):
    """
    _LogDB_

    LogDB object - interface to LogDB functionality.
    """
    def __init__(self, logger = None, **params):

        self.params = params
        self.logger = logger

        # config argument (within params) shall be reference to
        # Configuration instance
        self.config = params.get("Config", None)
        if  'CouchUrl' not in self.params:
            self.params.setdefault('CouchUrl', os.environ.get('COUCHURL', ''))
        if  'CentralCouchUrl' not in self.params:
            self.params.setdefault('CentralCouchUrl', os.environ.get('CENTRALCOUCHURL', ''))
        for attr in ['CouchUrl', 'CentralCouchUrl']:
            if  not self.params.get(attr):
                raise RuntimeError, '%s config value mandatory' % attr
        self.params.setdefault('DbName', 'logdb')

        self.backend = LogDBBackend(self.params['CouchUrl'],
                self.params['DbName'], logger=self.logger)
        self.central = LogDBBackend(self.params['CentralCouchUrl'],
                self.params['DbName'], logger=self.logger)

        if  self.logger:
            self.logger.debug("LogDB created successfully")

    def post(self, request, agent, msg, mtype="comment"):
        """Post new entry into LogDB for given request/agent pair"""
        res = self.backend.post(request, agent, msg, mtype)
        if  self.logger:
            self.logger.debug("LogDB post request, res=%s", res)
        return res

    def get(self, request, agent, mtype="comment"):
        """Retrieve all entries from LogDB for given request/agent pair"""
        res = self.backend.get(request, agent, mtype)
        if  self.logger:
            self.logger.debug("LogDB get request, res=%s", res)
        return res

    def delete(self, request, agent):
        """Delete entry in LogDB for given request/agent pair"""
        res = self.backend.delete(request, agent)
        if  self.logger:
            self.logger.debug("LogDB delete request, res=%s", res)
        return res

    def summary(self, request, agent):
        """Generate summary document for given request/agent pair"""
        res = self.backend.summary(request, agent)
        if  self.logger:
            self.logger.debug("LogDB summary request, res=%s", res)
        return res

    def upload2central(self, request, agent):
        """
        Upload local LogDB docs corresponding to given request/agent
        into central LogDB database
        """
        docs = self.backend.summary(request, agent)
        for doc in docs:
            self.central.db.queue(doc)
        res = self.central.db.commit()
        if  self.logger:
            self.logger.debug("LogDB upload2central request, res=%s", res)
        return res
