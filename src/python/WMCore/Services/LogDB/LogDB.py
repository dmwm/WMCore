#!/usr/bin/env python
"""
LogDB provides functionality to post/search messages into LogDB.
https://github.com/dmwm/WMCore/issues/5705
"""

# standard modules
import os
import re
import logging
import threading

# project modules
from WMCore.Services.LogDB.LogDBBackend import LogDBBackend, clean_entry
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Database.CMSCouch import CouchNotFoundError

class LogDB(object):
    """
    _LogDB_

    LogDB object - interface to LogDB functionality.
    """
    def __init__(self, url, identifier, logger=None, **kwds):
        self.logger = logger if logger else logging.getLogger()
        if  not url or not identifier:
            raise RuntimeError("Attempt to init LogDB with url='%s', identifier='%s'"\
                    % (url, identifier))
        self.identifier = identifier
        try:
            self.thread_name = kwds.pop('thread_name')
        except KeyError:
            self.thread_name = threading.currentThread().getName()
        self.url = url
        self.user_pat = re.compile(r'^/[a-zA-Z][a-zA-Z0-9/\=\s()\']*\=[a-zA-Z0-9/\=\.\-_/#:\s\']*$')
        self.agent = 0 if self.user_pat.match(self.identifier) else 1
        couch_url, db_name = splitCouchServiceURL(self.url)
        self.backend = LogDBBackend(couch_url, db_name, identifier, \
                self.thread_name, agent=self.agent, **kwds)
        self.logger.info(self)

    def __repr__(self):
        "Return representation for class"
        return "<LogDB(url=%s, identifier=%s, agent=%1)>" \
                % (self.url, self.identifier, self.agent)

    def post(self, request, msg, mtype="comment"):
        """Post new entry into LogDB for given request"""
        try:
            if  self.user_pat.match(self.identifier):
                res = self.backend.user_update(request, msg, mtype)
            else:
                res = self.backend.agent_update(request, msg, mtype)
        except Exception as exc:
            self.logger.error("LogDBBackend post API failed, error=%s" % str(exc))
            res = 'post-error'
        self.logger.debug("LogDB post request, res=%s", res)
        return res

    def get(self, request, mtype="comment"):
        """Retrieve all entries from LogDB for given request"""
        res = []
        try:
            for row in self.backend.get(request, mtype).get('rows', []):
                request = row['doc']['request']
                identifier = row['doc']['identifier']
                thr = row['doc']['thr']
                mtype = row['doc']['type']
                for rec in row['doc']['messages']:
                    rec.update({'request':request, 'identifier':identifier, 'thr': thr, 'type':mtype})
                    res.append(rec)
        except Exception as exc:
            self.logger.error("LogDBBackend get API failed, error=%s" % str(exc))
            res = 'get-error'
        self.logger.debug("LogDB get request, res=%s", res)
        return res

    def get_all_requests(self):
        """Retrieve all entries from LogDB for given request"""
        try:
            results = self.backend.get_all_requests()
            res = []
            for row in results['rows']:
                res.append(row["key"])
        except Exception as exc:
            self.logger.error("LogDBBackend get_all_requests API failed, error=%s" % str(exc))
            res = 'get-error'
        self.logger.debug("LogDB get_all_requests request, res=%s", res)
        return res

    def delete(self, request):
        """Delete entry in LogDB for given request"""
        try:
            res = self.backend.delete(request)
        except Exception as exc:
            self.logger.error("LogDBBackend delete API failed, error=%s" % str(exc))
            res = 'delete-error'
        self.logger.debug("LogDB delete request, res=%s", res)
        return res

    def cleanup(self, thr, backend='local'):
        """Clean-up back-end LogDB"""
        try:
            self.backend.cleanup(thr)
        except Exception as exc:
            self.logger.error('LogDBBackend cleanup API failed, backend=%s, error=%s' \
                    % (backend, str(exc)))
