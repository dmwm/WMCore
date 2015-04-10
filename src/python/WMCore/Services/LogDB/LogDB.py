#!/usr/bin/env python
"""
LogDB provides functionality to post/search messages into LogDB.
https://github.com/dmwm/WMCore/issues/5705
"""

# standard modules
import os
import logging

# project modules
from WMCore.Services.LogDB.LogDBBackend import LogDBBackend
from WMCore.Lexicon import splitCouchServiceURL

class LogDB(object):
    """
    _LogDB_

    LogDB object - interface to LogDB functionality.
    """
    def __init__(self, url, identifier, centralurl=None, logger=None, **kwds):
        self.logger = logger if logger else logging.getLogger()
        if  not url or not identifier:
            raise RuntimeError("Attempt to init LogDB with url='%s', identifier='%s'"\
                    % (url, identifier))
        self.identifier = identifier
        self.agent = 1 if centralurl else 0
        self.localurl = url
        self.centralurl = centralurl
        couch_url, db_name = splitCouchServiceURL(self.localurl)
        self.backend = LogDBBackend(couch_url, db_name, identifier, self.agent, **kwds)
        self.central = None
        if  centralurl:
            couch_url, db_name = splitCouchServiceURL(self.centralurl)
            self.central = LogDBBackend(couch_url, db_name, identifier, self.agent, **kwds)
        self.logger.info(self)

    def __repr__(self):
        "Return representation for class"
        return "<LogDB(local=%s, central=%s, agent=%s)>" \
                % (self.localurl, self.centralurl, self.agent)

    def post(self, request, msg, mtype="comment"):
        """Post new entry into LogDB for given request"""
        res = self.backend.post(request, msg, mtype)
        if  self.logger:
            self.logger.debug("LogDB post request, res=%s", res)
        return res

    def get(self, request, mtype="comment"):
        """Retrieve all entries from LogDB for given request"""
        res = self.backend.get(request, mtype)
        if  self.logger:
            self.logger.debug("LogDB get request, res=%s", res)
        return res

    def delete(self, request):
        """Delete entry in LogDB for given request"""
        res = self.backend.delete(request)
        if  self.logger:
            self.logger.debug("LogDB delete request, res=%s", res)
        return res

    def summary(self, request):
        """Generate summary document for given request"""
        res = self.backend.summary(request)
        if  self.logger:
            self.logger.debug("LogDB summary request, res=%s", res)
        return res

    def upload2central(self, request):
        """
        Upload local LogDB docs corresponding to given request
        into central LogDB database
        """
        if  not self.central:
            if  self.logger:
                self.logger.debug("LogDB upload2central does nothing, no central setup")
            return -1
        docs = self.backend.summary(request)
        for doc in docs:
            self.central.db.queue(doc)
        res = self.central.db.commit()
        if  self.logger:
            self.logger.debug("LogDB upload2central request, res=%s", res)
        return res
