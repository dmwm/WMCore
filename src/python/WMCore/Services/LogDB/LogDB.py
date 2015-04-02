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
    def __init__(self, config=None, logger=None, **params):

        self.config = config
        self.logger = logger
        self.params = params
        default_couch = os.environ.get('COUCHURL', '')
        default_central_couch = os.environ.get('CENTRALCOUCHURL', '')
        default_db_name = 'logdb'
        if  config:
            config = config.dictionary_()
            if  'logdb' in config:
                lconfig = config.get('logdb', {})
                if not isinstance(lconfig, dict):
                    lconfig = lconfig.dictionary_()
                self.params['CouchUrl'] = lconfig.get('couch_url', default_couch)
                self.params['CentralCouchUrl'] = lconfig.get('couch_url', default_central_couch)
                self.params['DbName'] = lconfig.get('db_name', default_db_name)

        if  'CouchUrl' not in self.params:
            self.params.setdefault('CouchUrl', default_couch)
        if  'CentralCouchUrl' not in self.params:
            self.params.setdefault('CentralCouchUrl', default_central_couch)
        for attr in ['CouchUrl', 'CentralCouchUrl']:
            if  not self.params.get(attr):
                raise RuntimeError, '%s config value mandatory' % attr
        if  'DbName' not in self.params:
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
