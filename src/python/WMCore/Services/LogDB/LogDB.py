#!/usr/bin/env python
"""
LogDB provides functionality to post/search messages into LogDB.
https://github.com/dmwm/WMCore/issues/5705
"""
# futures
from future import standard_library
standard_library.install_aliases()

# standard modules
import logging
import re
import threading
from collections import defaultdict
from http.client import HTTPException

# project modules
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.LogDB.LogDBBackend import LogDBBackend


def getLogDBInstanceFromThread():
    """This function only gets to call when LogDB is instantiated before hand
       All the WMComponentWorkers instatntiate LogDB automatically
    """
    myThread = threading.currentThread()
    if not hasattr(myThread, "logdbClient") or not isinstance(myThread.logdbClient, LogDB):
        # logdb is not set do anything
        return None
    return myThread.logdbClient


class LogDB(object):
    """
    _LogDB_

    LogDB object - interface to LogDB functionality.
    """

    def __init__(self, url, identifier, logger=None, **kwds):
        self.logger = logger if logger else logging.getLogger()
        self.url = url if url else 'https://cmsweb.cern.ch/couchdb/wmstats_logdb'
        self.identifier = identifier if identifier else 'unknown'
        self.default_user = "HEARTBEAT"

        try:
            self.thread_name = kwds.pop('thread_name')
        except KeyError:
            self.thread_name = threading.currentThread().getName()

        self.user_pat = re.compile(r'^/[a-zA-Z][a-zA-Z0-9/\=\s()\']*\=[a-zA-Z0-9/\=\.\-_/#:\s\']*$')
        self.agent = 0 if self.user_pat.match(self.identifier) else 1
        couch_url, db_name = splitCouchServiceURL(self.url)
        self.backend = LogDBBackend(couch_url, db_name, identifier,
                                    self.thread_name, agent=self.agent, **kwds)
        self.logger.info(self)

    def __repr__(self):
        "Return representation for class"
        return "<LogDB(url=%s, identifier=%s, agent=%d)>" \
               % (self.url, self.identifier, self.agent)

    def post(self, request=None, msg="", mtype="comment"):
        """Post new entry into LogDB for given request"""
        res = 'post-error'
        try:
            if request is None:
                request = self.default_user
            if self.user_pat.match(self.identifier):
                res = self.backend.user_update(request, msg, mtype)
            else:
                res = self.backend.agent_update(request, msg, mtype)
        except HTTPException as ex:
            msg = "Failed to post doc to LogDB. Reason: %s, status: %s" % (ex.reason, ex.status)
            self.logger.error(msg)
        except Exception as exc:
            self.logger.error("LogDBBackend post API failed, error=%s", str(exc))
        self.logger.debug("LogDB post request, res=%s", res)
        return res

    def get(self, request=None, mtype=None):
        """Retrieve all entries from LogDB for given request"""
        res = []
        try:
            if request is None:
                request = self.default_user
            if self.user_pat.match(self.identifier):
                agent = False
            else:
                agent = True
            for row in self.backend.get(request, mtype, agent=agent).get('rows', []):
                request = row['doc']['request']
                identifier = row['doc']['identifier']
                thr = row['doc']['thr']
                mtype = row['doc']['type']
                for rec in row['doc']['messages']:
                    rec.update({'request': request, 'identifier': identifier, 'thr': thr, 'type': mtype})
                    res.append(rec)
        except HTTPException as ex:
            msg = "Failed to get doc from LogDB. Reason: %s, status: %s" % (ex.reason, ex.status)
            self.logger.error(msg)
            res = 'get-error'
        except Exception as exc:
            self.logger.error("LogDBBackend get API failed, error=%s", str(exc))
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
            self.logger.error("LogDBBackend get_all_requests API failed, error=%s", str(exc))
            res = 'get-error'
        self.logger.debug("LogDB get_all_requests request, res=%s", res)
        return res

    def delete(self, request=None, mtype=None, this_thread=False, agent=True):
        """
        Delete entry in LogDB for given request
        if mtype == None - delete all the log for that request
        mtype != None - only delete specified mtype
        """
        res = 'delete-error'
        try:
            if request is None:
                request = self.default_user
            res = self.backend.delete(request, mtype, this_thread, agent)
        except HTTPException as ex:
            msg = "Failed to delete doc in LogDB. Reason: %s, status: %s" % (ex.reason, ex.status)
            self.logger.error(msg)
        except Exception as ex:
            self.logger.error("LogDBBackend delete API failed, error=%s", str(ex))
        self.logger.debug("LogDB delete request, res=%s", res)
        return res

    def cleanup(self, thr, backend='local'):
        """Clean-up back-end LogDB"""
        docs = []
        try:
            docs = self.backend.cleanup(thr)
        except Exception as exc:
            self.logger.error('LogDBBackend cleanup API failed, backend=%s, error=%s', backend, str(exc))
        return docs

    def heartbeat_report(self):
        report = defaultdict(dict)
        if self.user_pat.match(self.identifier):
            self.logger.error("User %s: doesn't allow this function", self.identifier)
            return report

        for row in self.backend.get(self.default_user, None, agent=True).get('rows', []):
            identifier = row['doc']['identifier']
            # wmstats thread can run in multiple boxed.
            if self.identifier == identifier or identifier.startswith(self.identifier):
                # this will handle wmstats DataCacheUpdate thread with multiple machine
                postfix = identifier.replace(self.identifier, "")
                thr = "%s%s" % (row['doc']['thr'], postfix)
                mtype = row['doc']['type']
                ts = row['doc']['messages'][-1]['ts']
                msg = row['doc']['messages'][-1]['msg']
                if (thr in report) and ('ts' in report[thr]) and ts <= report[thr]['ts']:
                    continue
                else:
                    report[thr]['type'] = mtype
                    report[thr]['msg'] = msg
                    report[thr]['ts'] = ts
        return report

    def _append_down_component_detail(self, report, thr, msg, ts=0, state="error"):
        report['down_components'].append(thr)
        detail = {'name': thr, 'worker_name': thr, 'state': state,
                  'last_error': ts, 'error_message': msg,
                  'pid': 'N/A'}
        report['down_component_detail'].append(detail)
        return

    def wmstats_down_components_report(self, thread_list):
        report = {}
        report['down_components'] = []
        report['down_component_detail'] = []

        hbinfo = self.heartbeat_report()
        for thr in thread_list:
            # skip DataCacheUpdate thread. It will have multiple with post fix.
            # i.e. DataCacheUpdate-vocms111
            # TODO, need a better way to check
            if thr != "DataCacheUpdate" and thr not in hbinfo:
                self._append_down_component_detail(report, thr, "Thread not running")

        for thr, info in hbinfo.iteritems():
            if info['type'] == 'agent-error':
                self._append_down_component_detail(report, thr, info['msg'], info['ts'])
        return report
