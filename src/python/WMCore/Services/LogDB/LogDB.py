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
        self.params.setdefault('CouchUrl', os.environ.get('COUCHURL'))
        if not self.params.get('CouchUrl'):
            raise RuntimeError, 'CouchUrl config value mandatory'
        self.params.setdefault('DbName', 'logdb')

        self.backend = LogDBBackend(self.params['CouchUrl'],
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
