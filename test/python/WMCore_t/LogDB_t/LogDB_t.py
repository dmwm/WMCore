#!/usr/bin/env python
"""
_LogDB_t_

LogDB tests
"""

import os
import time
import logging
import unittest
import threading

from WMCore.Services.LogDB.LogDB import LogDB

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
        localurl = 'http://localhost:5984/locallogdb_t'
        centralurl = 'http://localhost:5984/globallogdb_t'
        identifier = 'agent-db'
        self.localdb = LogDB(localurl, identifier, centralurl, logger=self.logger, create=True)
        identifier = 'central-db'
        self.globaldb = LogDB(localurl, identifier, logger=self.logger, create=True)

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.localdb.backend.deleteDatabase()
        self.globaldb.backend.deleteDatabase()

    def test_apis(self):
        "Test LogDB APIs"
        request = 'abc'

        self.assertEqual(self.localdb.agent, 1)
        mtype = self.localdb.backend.prefix('msg')
        self.assertEqual(mtype, 'agent-msg')

        self.assertEqual(self.globaldb.agent, 0)
        mtype = self.globaldb.backend.prefix('msg')
        self.assertEqual(mtype, 'msg')

        # if we post messages for the same request only last one should survive
        self.localdb.post(request, 'msg1')
        self.localdb.post(request, 'msg2')
        self.localdb.post(request, 'msg3')
        self.localdb.post(request, 'msg4')
        docs = self.localdb.summary(request)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]['msg'], 'msg4')

        # add message to global logdb
        # upload local messages into global logdb
        # we should have only two messages now, doc1 and msg4
        self.globaldb.post(request, 'doc1')
        self.localdb.upload2central(request)
        docs = self.globaldb.summary(request)
        self.assertEqual(len(docs), 2)

        res = self.globaldb.upload2central(request)
        self.assertEqual(res, -1) # does nothing

    def test_cleanup(self):
        "Test clean-up LogDB API"
        request = 'abc'
        self.localdb.post(request, 'msg1', 'info')
        self.localdb.post(request, 'msg2', 'comment')
        self.localdb.post(request, 'msg3', 'warning')
        self.localdb.post(request, 'msg4', 'error')
        all_docs = self.localdb.summary(request)
        self.localdb.backend.cleanup(thr=-10) # look into past
        past_docs = self.localdb.summary(request)
        self.assertEqual(len(all_docs), len(past_docs))
        self.localdb.backend.cleanup(thr=10) # look into future
        docs = self.localdb.summary(request)
        self.assertEqual(len(docs), 0)

if __name__ == "__main__":
    unittest.main()
