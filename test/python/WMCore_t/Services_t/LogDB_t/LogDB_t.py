#!/usr/bin/env python
"""
_LogDB_t_

LogDB tests
"""
from __future__ import print_function

import time
import logging
import unittest

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.Services.LogDB.LogDBBackend import gen_hash
from WMCore.Services.LogDB.LogDBExceptions import LogDBError
from WMCore.Services.LogDB.LogDBReport import LogDBReport

class LogDBTest(unittest.TestCase):
    """
    _LogDBTest_

    """
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.
        """
        logging.basicConfig()
        self.logger = logging.getLogger('LogDBTest')
#        self.logger.setLevel(logging.DEBUG)

        self.schema = []
        self.couchApps = ["LogDB"]
        self.testInit = TestInitCouchApp('LogDBTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        dbName = 'logdb_t'
        self.testInit.setupCouch(dbName, *self.couchApps)
        url = "%s/%s" % (self.testInit.couchUrl, dbName)

        identifier = 'agentname'
        self.agent_inst = LogDB(url, identifier, logger=self.logger, thread_name="MainThread")
        self.agent_inst2 = LogDB(url, identifier, logger=self.logger, thread_name="Test")
        identifier = '/DC=org/DC=doegrids/OU=People/CN=First Last 123'
        self.user_inst = LogDB(url, identifier, logger=self.logger, thread_name="MainThread")
        self.report = LogDBReport(self.agent_inst)

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()

    def test_docid(self):
        "Test docid API"
        request = 'abs'
        mtype = self.agent_inst.backend.prefix('info')
        res = self.agent_inst.backend.docid(request, mtype)
        key = '--'.join([request, self.agent_inst.identifier, self.agent_inst.thread_name, mtype])
        khash = gen_hash(key)
        self.assertEqual(res, khash)

    def test_prefix(self):
        "Test prefix LogDBBackend API"

        self.assertEqual(self.agent_inst.agent, 1)
        mtype = self.agent_inst.backend.prefix('msg')
        self.assertEqual(mtype, 'agent-msg')

        self.assertEqual(self.user_inst.agent, 0)
        mtype = self.user_inst.backend.prefix('msg')
        self.assertEqual(mtype, 'msg')

    def test_apis(self):
        "Test LogDB APIs"
        request = 'abc'

        # if we post messages for the same request only last one should survive
        self.agent_inst.post(request, 'msg1')
#        time.sleep(1)
        self.agent_inst.post(request, 'msg2')
#        time.sleep(1)
        self.agent_inst.post(request, 'msg3')
#        time.sleep(1)
        self.agent_inst.post(request, 'msg4')
#        time.sleep(1)
        docs = self.agent_inst.get(request)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]['msg'], 'msg4')

        # add message as user, user based messages will grow
        self.user_inst.post(request, 'doc1')
#        time.sleep(1)
        docs = self.user_inst.get(request)
        self.assertEqual(len(docs), 2) # we have msg4 and doc1
        self.user_inst.post(request, 'doc1') # we have msg2 and two doc1 messages
#        time.sleep(1)
        docs = self.user_inst.get(request)
        self.assertEqual(len(docs), 3)

    def test_cleanup(self):
        "Test clean-up LogDB API"
        request = 'abc'
        self.agent_inst.post(request, 'msg1', 'info')
        self.agent_inst.post(request, 'msg2', 'comment')
        self.agent_inst.post(request, 'msg3', 'warning')
        self.agent_inst.post(request, 'msg4', 'error')
        all_docs = self.agent_inst.get(request)
        self.agent_inst.backend.cleanup(thr=10) # look into past
        past_docs = self.agent_inst.get(request)
        self.assertEqual(len(all_docs), len(past_docs))
        self.agent_inst.backend.cleanup(thr=-10) # look into future
        docs = self.agent_inst.get(request)
        self.assertEqual(len(docs), 0)

    def test_get_all_requests(self):
        "Test get_all_requests LogDB API"
        request = 'abc'
        self.agent_inst.post(request, 'msg1', 'info')
        self.agent_inst.post(request, 'msg2', 'info')
        self.agent_inst.post(request, 'msg3', 'warning')
        self.agent_inst.post(request, 'msg4', 'error')
        sum_docs = self.agent_inst.get(request)
        self.assertEqual(len(sum_docs), 3) # since we have two info records
        request = 'abc2'
        self.agent_inst.post(request, 'msg1', 'info')
        all_docs = self.agent_inst.get_all_requests()
        self.assertEqual(len(all_docs), 2) # we only have two distinct request names

    def test_delete(self):
        "Test delete LogDB API"
        request = 'abc'
        self.agent_inst.post(request, 'msg1', 'info')
        self.agent_inst.delete(request)
        all_docs = self.agent_inst.get_all_requests()
        self.assertEqual(len(all_docs), 0)

    def test_get(self):
        "Test get LogDB API"
        request = 'abc'
        self.agent_inst2.post(request, 'msg1', 'info') # doc with different thread name
        self.agent_inst.post(request, 'msg1', 'info')
        self.agent_inst.post(request, 'msg2', 'info')
        self.agent_inst.post(request, 'msg3', 'warning')
        self.agent_inst.post(request, 'msg4', 'error')
        docs = self.agent_inst.get(request)
        self.assertEqual(len(docs), 4) # there should be 4 docs one for Test thread and 3 for MainThread
        docs = self.agent_inst2.get(request)
        self.assertEqual(len(docs), 4) # this should result the same as above.

        docs1 = self.agent_inst.get(request, 'info')
        docs2 = self.agent_inst.get(request, 'info')
        req1 = set([r['request'] for r in docs1])
        self.assertEqual(len(req1), 1)
        req2 = set([r['request'] for r in docs2])
        self.assertEqual(len(req2), 1)
        self.assertEqual(req1, req2)

    def test_thread_name(self):
        "Test thread name support by LogDB APIs"
        request = 'abc'
        self.agent_inst2.post(request, 'msg1', 'info') # doc with different thread name
        self.agent_inst.post(request, 'msg1', 'info')
        self.agent_inst.post(request, 'msg2', 'info')
        self.agent_inst.post(request, 'msg3', 'warning')
        self.agent_inst.post(request, 'msg4', 'error')
        maindocs = self.agent_inst.get(request)
        self.assertEqual(len(maindocs), 4) # since we have two info records in agent_inst
        thr_docs = self.agent_inst2.get(request)
        self.assertEqual(len(thr_docs), 4) # number of docs in different thread
        req1 = set([r['request'] for r in maindocs])
        self.assertEqual(len(req1), 1)
        req2 = set([r['request'] for r in thr_docs])
        self.assertEqual(len(req2), 1)
        self.assertEqual(req1, req2)
        worker1 = set([r['thr'] for r in maindocs])
        self.assertEqual(worker1, set(["Test", "MainThread"]))
        worker2 = set([r['thr'] for r in thr_docs])
        self.assertEqual(worker2, set(["Test", "MainThread"]))

        docs = set([r['identifier'] for r in maindocs])
        self.assertEqual(docs, set([self.agent_inst.identifier]))
        docs = set([r['identifier'] for r in thr_docs])
        self.assertEqual(docs, set([self.agent_inst.identifier]))

    def test_checks(self):
        "Tests LogDB check/mtype functionality"
        request = 'abc'
        res = self.agent_inst.post(request, 'msg', 'ingo') # should be silent, i.o. no Exceptions
        self.assertEqual('post-error', res)
        args = (request, 'msg', 'ingo') # wrong type
        self.assertRaises(LogDBError, self.agent_inst.backend.agent_update, *args)
        args = (request, 'ingo') # wrong type
        self.assertRaises(LogDBError, self.agent_inst.backend.check, *args)
        args = ('', 'msg', 'info') # empty request string
        self.assertRaises(LogDBError, self.agent_inst.backend.agent_update, *args)
        args = ('', 'info') # empty request string
        self.assertRaises(LogDBError, self.agent_inst.backend.check, *args)

    def test_report(self):
        "Test LogDBReport APIs"
        request = 'abc'
        self.agent_inst.post(request, 'msg1', 'info')
        time.sleep(1)
        self.agent_inst.post(request, 'msg2', 'comment')
        time.sleep(1)
        self.agent_inst.post(request, 'msg3', 'warning')
        time.sleep(1)
        self.agent_inst.post(request, 'msg4', 'error')
        time.sleep(1)
        self.report.to_stdout(request)
        out = self.report.to_txt(request)
        print("\n### txt report\n", out)
        out = self.report.to_html(request)
        print("\n### html report\n", out)

        docs = self.agent_inst.get(request)
        tdocs = self.report.orderby(docs, order='ts')
        messages = [d['msg'] for d in tdocs]
        self.assertEqual(messages, ['msg4', 'msg3', 'msg2', 'msg1'])

    def test_heartbeat(self):
        "Test heartbeat_report function"
        threadName = 'MainThread'
        failThread = 'FailThread'
        self.agent_inst.post(msg="Test heartbeat", mtype="info")
        time.sleep(1)
        self.agent_inst.post(msg="Test heartbeatError", mtype="error")
        report = self.agent_inst.heartbeat_report()
        self.assertEqual(report[threadName]['type'], 'agent-error')

        report = self.agent_inst.wmstats_down_components_report([threadName])
        self.assertEqual(report['down_components'], [threadName])

        self.agent_inst.post(msg="Test heartbeat", mtype="info")
        report = self.agent_inst.wmstats_down_components_report([threadName])
        self.assertEqual(report['down_components'], [])

        report = self.agent_inst.wmstats_down_components_report([threadName, failThread])
        self.assertEqual(report['down_components'], [failThread])

        self.agent_inst.post(msg="Test heartbeatError2", mtype="error")
        report = self.agent_inst.wmstats_down_components_report([threadName])
        self.assertEqual(report['down_components'], [threadName])

        self.agent_inst.delete(mtype="error", this_thread=True)
        report = self.agent_inst.wmstats_down_components_report([threadName])
        self.assertEqual(report['down_components'], [])

if __name__ == "__main__":
    unittest.main()
