#!/usr/bin/env python
"""
This unit test isn't really a unit test, more an explaination of how to use the
CMSCouch library to work with CouchDB. It currently assumes you have a running
CouchDB instance, and is not going to work in an automated way just yet - we'll
need to add Couch as an external, include it in start up scripts etc.
"""
import unittest
import os
import hashlib
import base64
import time
from Utils.Utilities import encodeUnicodeToBytes
from WMCore.Database.CMSCouch import (CouchServer, CouchMonitor, Document, Database,
                                      CouchInternalServerError, CouchNotFoundError)


class CMSCouchTest(unittest.TestCase):

    def setUp(self):
        # Make an instance of the server
        creds = os.getenv("COUCHURL", 'http://admin:password@localhost:5984')
        self.server = CouchServer(creds)
        testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        self.testdbname = 'cmscouch_unittest_%s' % testname.lower()

        if self.testdbname in self.server.listDatabases():
            self.server.deleteDatabase(self.testdbname)

        self.server.createDatabase(self.testdbname)
        self.db = self.server.connectDatabase(self.testdbname)
        self.replicatordb = self.server.connectDatabase('_replicator')

    def tearDown(self):
        """Destroy our testing setup"""
        if self.testdbname in self.server.listDatabases():
            self.server.deleteDatabase(self.testdbname)

    def testCommitOne(self):
        # Can I commit one dict
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc)[0]['id']
        # What about a Document
        doc = Document(inputDict = doc)
        id = self.db.commitOne(doc)[0]['id']

    def testCommitOneWithQueue(self):
        """
        CommitOne bypasses the queue, but it should maintain the queue if
        present for a future call to commit.
        """
        # Queue up five docs
        doc = {'foo':123, 'bar':456}
        for i in range(1,6):
            self.db.queue(doc)
        # Commit one Document
        doc = Document(inputDict = doc)
        id = self.db.commitOne(doc)[0]['id']
        self.assertEqual(1, len(self.db.allDocs()['rows']))
        self.db.commit()
        self.assertEqual(6, len(self.db.allDocs()['rows']))

    def testTimeStamping(self):
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc, timestamp=True)[0]['id']
        doc = self.db.document(id)
        self.assertTrue('timestamp' in doc.keys())

    def testDeleteDoc(self):
        doc = {'foo':123, 'bar':456}
        self.db.commitOne(doc)
        all_docs = self.db.allDocs()
        self.assertEqual(1, len(all_docs['rows']))

        # The db.delete_doc is immediate
        id = all_docs['rows'][0]['id']
        self.db.delete_doc(id)
        all_docs = self.db.allDocs()
        self.assertEqual(0, len(all_docs['rows']))

    def testDeleteQueuedDocs(self):
        doc1 = {'foo':123, 'bar':456}
        doc2 = {'foo':789, 'bar':101112}
        self.db.queue(doc1)
        self.db.queue(doc2)
        self.db.commit()

        all_docs = self.db.allDocs()
        self.assertEqual(2, len(all_docs['rows']))
        for res in all_docs['rows']:
            id = res['id']
            doc = self.db.document(id)
            self.db.queueDelete(doc)
        all_docs = self.db.allDocs()
        self.assertEqual(2, len(all_docs['rows']))

        self.db.commit()

        all_docs = self.db.allDocs()
        self.assertEqual(0, len(all_docs['rows']))

    def testReplicate(self):
        repl_db = self.server.connectDatabase(self.db.name + 'repl')

        doc_id = self.db.commitOne({'foo':123}, timestamp=True)[0]['id']
        doc_v1 = self.db.document(doc_id)

        #replicate
        resp = self.server.replicate(self.db.name, repl_db.name, sleepSecs=5)
        self.assertTrue(resp["ok"])
        self.assertTrue("id" in resp)
        self.assertTrue("rev" in resp)

        # wait for a few seconds to replication to be triggered.
        time.sleep(1)
        self.assertEqual(self.db.document(doc_id), repl_db.document(doc_id))
        self.server.deleteDatabase(repl_db.name)

    def testSlashInDBName(self):
        """
        Slashes are a valid character in a database name, and are useful as it
        creates a directory strucutre for the couch data files.
        """
        db_name = 'wmcore/unittests'
        try:
            self.server.deleteDatabase(db_name)
        except:
            # Ignore this - the database shouldn't already exist
            pass

        db = self.server.createDatabase(db_name)
        info = db.info()
        assert info['db_name'] == db_name

        db_name = 'wmcore/unittests'
        db = self.server.connectDatabase(db_name)
        info = db.info()
        assert info['db_name'] == db_name

        db = Database(db_name, url = os.environ["COUCHURL"])
        info = db.info()
        assert info['db_name'] == db_name

        self.server.deleteDatabase(db_name)

    def testInvalidName(self):
        """
        Capitol letters are not allowed in database names.
        """
        db_name = 'Not A Valid Name'
        self.assertRaises(ValueError, self.server.createDatabase, db_name)
        self.assertRaises(ValueError, self.server.deleteDatabase, db_name)
        self.assertRaises(ValueError, self.server.connectDatabase, db_name)
        self.assertRaises(ValueError, Database, db_name)

    def testDocumentSerialisation(self):
        """
        A document should be writable into the couchdb with a timestamp.
        """
        d = Document()
        d['foo'] = 'bar'
        doc_info = self.db.commit(doc=d, timestamp=True)[0]
        d_from_db = self.db.document(doc_info['id'])
        self.assertEqual(d['foo'], d_from_db['foo'])
        self.assertEqual(d['timestamp'], d_from_db['timestamp'])

    def testAttachments(self):
        """
        Test uploading attachments with and without checksumming
        """
        doc = self.db.commitOne({'foo':'bar'}, timestamp=True)[0]
        attachment1 = "Hello"
        attachment2 = "How are you today?"
        attachment3 = "I'm very well, thanks for asking"
        attachment4 = "Lovely weather we're having"
        attachment5 = "Goodbye"
        keyhash = hashlib.md5()
        keyhash.update(encodeUnicodeToBytes(attachment5))
        attachment5_md5 = keyhash.digest()
        attachment5_md5 = base64.b64encode(attachment5_md5)
        attachment6 = "Good day to you, sir!"
        #TODO: add a binary attachment - e.g. tar.gz
        doc = self.db.addAttachment(doc['id'], doc['rev'], attachment1)
        doc = self.db.addAttachment(doc['id'], doc['rev'], attachment2, contentType="foo/bar")
        doc = self.db.addAttachment(doc['id'], doc['rev'], attachment3, name="my_greeting")
        doc = self.db.addAttachment(doc['id'], doc['rev'], attachment4, add_checksum=True)
        doc = self.db.addAttachment(doc['id'], doc['rev'], attachment5, checksum=attachment5_md5)

        self.assertRaises(CouchInternalServerError, self.db.addAttachment, doc['id'], doc['rev'], attachment6, checksum='123')

    def testRevisionHandling(self):
        # This test won't work from an existing database, conflicts will be preserved, so
        # ruthlessly remove the databases to get a clean slate.
        try:
            self.server.deleteDatabase(self.db.name)
        except CouchNotFoundError:
            pass # Must have been deleted already

        try:
            self.server.deleteDatabase(self.db.name + 'repl')
        except CouchNotFoundError:
            pass # Must have been deleted already

        # I'm going to create a conflict, so need a replica db
        self.db = self.server.connectDatabase(self.db.name)
        repl_db = self.server.connectDatabase(self.db.name + 'repl')

        doc_id = self.db.commitOne({'foo':123}, timestamp=True)[0]['id']
        doc_v1 = self.db.document(doc_id)

        #replicate
        self.server.replicate(self.db.name, repl_db.name, sleepSecs=5)
        time.sleep(1)

        doc_v2 = self.db.document(doc_id)
        doc_v2['bar'] = 456
        doc_id_rev2 = self.db.commitOne(doc_v2)[0]
        doc_v2 = self.db.document(doc_id)

        #now update the replica
        conflict_doc = repl_db.document(doc_id)
        conflict_doc['bar'] = 101112
        repl_db.commitOne(conflict_doc)

        #replicate, creating the conflict
        self.server.replicate(self.db.name, repl_db.name, sleepSecs=5)
        time.sleep(1)

        tempDesignDoc = {'views': {
                             'conflicts': {
                                 'map': "function(doc) {if(doc._conflicts) {emit(doc._conflicts, null);}}"
                                           },
                                   }
                         }
        # create the temporary views, which in CouchDB 3.x must be through a
        # permanent view (within a design doc)
        repl_db.put('/%s/_design/TempDesignDoc' % repl_db.name, tempDesignDoc)
        self.db.put('/%s/_design/TempDesignDoc' % self.db.name, tempDesignDoc)

        # Should have one conflict in the repl database
        dataRepl = repl_db.get('/%s/_design/TempDesignDoc/_view/conflicts' % repl_db.name)
        self.assertEqual(dataRepl['total_rows'], 1)
        # Should have no conflicts in the source database
        data = self.db.get('/%s/_design/TempDesignDoc/_view/conflicts' % self.db.name)
        self.assertEqual(data['total_rows'], 0)
        self.assertTrue(repl_db.documentExists(dataRepl['rows'][0]['id'], rev=dataRepl['rows'][0]['key'][0]))

        repl_db.delete_doc(dataRepl['rows'][0]['id'], rev=dataRepl['rows'][0]['key'][0])
        dataRepl = repl_db.get('/%s/_design/TempDesignDoc/_view/conflicts' % repl_db.name)

        self.assertEqual(dataRepl['total_rows'], 0)
        self.server.deleteDatabase(repl_db.name)

        #update it again
        doc_v3 = self.db.document(doc_id)
        doc_v3['baz'] = 789
        doc_id_rev3 = self.db.commitOne(doc_v3)[0]
        doc_v3 = self.db.document(doc_id)

        #test that I can pull out an old revision
        doc_v1_test = self.db.document(doc_id, rev=doc_v1['_rev'])
        self.assertEqual(doc_v1, doc_v1_test)

        #test that I can check a revision exists
        self.assertTrue(self.db.documentExists(doc_id, rev=doc_v2['_rev']))

        self.assertFalse(self.db.documentExists(doc_id, rev='1'+doc_v2['_rev']))

        #why you shouldn't rely on rev
        self.db.compact(blocking=True)
        self.assertFalse(self.db.documentExists(doc_id, rev=doc_v1['_rev']))
        self.assertFalse(self.db.documentExists(doc_id, rev=doc_v2['_rev']))
        self.assertTrue(self.db.documentExists(doc_id, rev=doc_v3['_rev']))

    def testCommit(self):
        """
        Test queue and commit modes
        """
        # try to commit 2 random docs
        doc = {'foo':123, 'bar':456}
        self.db.queue(doc)
        self.db.queue(doc)
        self.assertEqual(2, len(self.db.commit()))

        # committing 2 docs with the same id will fail
        self.db.queue(Document(id = "1", inputDict = {'foo':123, 'bar':456}))
        self.db.queue(Document(id = "1", inputDict = {'foo':1234, 'bar':456}))
        answer = self.db.commit()
        self.assertEqual(2, len(answer))
        self.assertEqual(answer[0]['ok'], True)
        self.assertEqual(answer[1]['error'], 'conflict')

        # bulk commit with one conflict unresolved
        self.db.queue(Document(id = "2", inputDict = doc))
        self.db.queue(Document(id = "2", inputDict = {'foo':1234, 'bar':456}))
        answer = self.db.commit()
        self.assertEqual(2, len(answer))
        self.assertTrue('error' not in answer[0])
        self.assertTrue('error' in answer[1])
        self.assertEqual(answer[0]['id'], '2')
        self.assertEqual(answer[1]['id'], '2')

        # callbacks can do stuff when conflicts arise
        # this particular one just overwrites the document
        def callback(db, data, result):
            for doc in data['docs']:
                if doc['_id'] == result['id']:
                    doc['_rev'] = db.document(doc['_id'])['_rev']
                    retval = db.commitOne(doc)
            return retval[0]

        # bulk commit with one callback enabled (conflicts resolved)
        self.db.queue(Document(id = "2", inputDict = {'foo':5, 'bar':6}))
        self.db.queue(Document(id = "3", inputDict = doc))
        answer = self.db.commit(callback = callback)
        self.assertEqual(2, len(answer))
        self.assertTrue('error' not in answer[0])
        self.assertTrue('error' not in answer[1])
        self.assertEqual(answer[0]['id'], '2')
        self.assertEqual(answer[1]['id'], '3')
        updatedDoc = self.db.document('2')
        self.assertEqual(updatedDoc['foo'], 5)
        self.assertEqual(updatedDoc['bar'], 6)

        return

    def testUpdateHandler(self):
        """
        Test that update function support works
        """

        update_ddoc = {
            '_id':'_design/foo',
            'language': 'javascript',
            'updates':{
                "bump-counter" : 'function(doc, req) {if (!doc.counter) {doc.counter = 0};doc.counter += 1;return [doc,"bumped it!"];}',
            }
        }
        self.db.commit(update_ddoc)
        doc = {'foo': 123, 'counter': 0}
        doc_id = self.db.commit(doc)[0]['id']
        self.assertEqual("bumped it!", self.db.updateDocument(doc_id, 'foo', 'bump-counter'))

        self.assertEqual(1, self.db.document(doc_id)['counter'])


    def testList(self):
        """
        Test list function works ok
        """
        update_ddoc = {
            '_id':'_design/foo',
            'language': 'javascript',
            'views' : {
                       'all' : {
                                'map' : 'function(doc) {emit(null, null) }'
                                },
                       },
            'lists' : {
                'errorinoutput' : 'function(doc, req) {send("A string with the word error in")}',
                'malformed' : 'function(doc, req) {somethingtoraiseanerror}',
            }
        }
        self.db.commit(update_ddoc)
        # approriate errors raised
        self.assertRaises(CouchNotFoundError, self.db.loadList, 'foo', 'error', 'view_doesnt_exist')
        self.assertRaises(CouchInternalServerError, self.db.loadList, 'foo', 'malformed', 'all')
        # error in list output string shouldn't raise an error
        self.assertEqual(self.db.loadList('foo', 'errorinoutput', 'all'),
                         "A string with the word error in")

    def testAllDocs(self):
        """
        Test AllDocs with options
        """
        self.db.queue(Document(id = "1", inputDict = {'foo':123, 'bar':456}))
        self.db.queue(Document(id = "2", inputDict = {'foo':123, 'bar':456}))
        self.db.queue(Document(id = "3", inputDict = {'foo':123, 'bar':456}))

        self.db.commit()
        self.assertEqual(3, len(self.db.allDocs()['rows']))
        self.assertEqual(2, len(self.db.allDocs({'startkey': "2"})['rows']))
        self.assertEqual(2, len(self.db.allDocs(keys = ["1", "3"])['rows']))
        self.assertEqual(1, len(self.db.allDocs({'limit':1}, ["1", "3"])['rows']))
        self.assertTrue('error' in self.db.allDocs(keys = ["1", "4"])['rows'][1])

    def testUpdateBulkDocuments(self):
        """
        Test AllDocs with options
        """
        self.db.queue(Document(id="1", inputDict={'foo':123, 'bar':456}))
        self.db.queue(Document(id="2", inputDict={'foo':123, 'bar':456}))
        self.db.queue(Document(id="3", inputDict={'foo':123, 'bar':456}))
        self.db.commit()

        self.db.updateBulkDocumentsWithConflictHandle(["1", "2", "3"], {'foo': 333}, 2)
        result = self.db.allDocs({"include_docs": True})['rows']
        self.assertEqual(3, len(result))
        for item in result:
            self.assertEqual(333, item['doc']['foo'])

        self.db.updateBulkDocumentsWithConflictHandle(["1", "2", "3"], {'foo': 222}, 10)
        result = self.db.allDocs({"include_docs": True})['rows']
        self.assertEqual(3, len(result))
        for item in result:
            self.assertEqual(222, item['doc']['foo'])

    def testUpdateHandlerAndBulkUpdateProfile(self):
        """
        Test that update function support works
        """
        # for actual test increase the size value: For 10000 records, 96 sec vs 4 sec
        size = 100
        for i in range(size):
            self.db.queue(Document(id="%s" % i, inputDict={'name':123, 'counter':0}))

        update_doc = {
            '_id':'_design/foo',
            'language': 'javascript',
            'updates':{
                "change-counter" : """function(doc, req) { if (doc) { var data = JSON.parse(req.body);
                                      for (var field in data) {doc.field = data.field;} return [doc, 'OK'];}}""",
            }
        }

        self.db.commit(update_doc)
        start = time.time()
        for id in range(size):
            doc_id = "%s" % id
            self.db.updateDocument(doc_id, 'foo', 'change-counter', {'counter': 1}, useBody=True)
        end = time.time()

        print("update handler: %s sec" % (end - start))

        start = time.time()
        ids = []
        for id in range(size):
            doc_id = "%s" % id
            ids.append(doc_id)
        self.db.updateBulkDocumentsWithConflictHandle(ids, {'counter': 2}, 1000)
        end = time.time()

        print("bulk update: %s sec" % (end - start))

    def testGetWelcome(self):
        """Test the 'getCouchWelcome' method"""
        resp = self.server.getCouchWelcome()
        self.assertEqual(resp["couchdb"], "Welcome")
        self.assertTrue("vendor" in resp)
        # For CouchDB 1.6.1, expected output is like:
        # {"couchdb":"Welcome","uuid":"2fe221d694085fa52409a9dc59d9f3f0","version":"1.6.1",
        # "vendor":{"version":"1.6.1","name":"The Apache Software Foundation"}}
        # Different output between CouchDB 1.6 vs CouchDB 3.1
        if resp["version"] != "1.6.1":
            self.assertTrue("features" in resp)

class CouchMonitorTest(unittest.TestCase):
    """
    Tests for the CouchMonitor class
    """

    def setUp(self):
        # Make an instance of the server
        self.couchUrl = os.getenv("COUCHURL", 'http://admin:password@localhost:5984')
        self.monitor = CouchMonitor(self.couchUrl)
        # Create a database, drop an existing one first
        self.dbNames = ['couchmonitor_unittest1', 'couchmonitor_unittest2']
        for dbName in self.dbNames:
            if dbName in self.monitor.couchServer.listDatabases():
                self.monitor.couchServer.deleteDatabase(dbName)

        for dbName in self.dbNames:
            self.monitor.couchServer.createDatabase(dbName)

        # define source/target endpoints
        self.sourceUrl = f"{self.couchUrl}/{self.dbNames[0]}"
        self.targetUrl = f"{self.couchUrl}/{self.dbNames[1]}"

    def tearDown(self):
        """Destroy the test databases before starting the next test"""
        for dbName in self.dbNames:
            self.monitor.couchServer.deleteDatabase(dbName)

    def testGetActiveTasks(self):
        """Test the 'getActiveTasks' method"""
        resp = self.monitor.getActiveTasks()
        # we haven't defined any task yet
        self.assertEqual(resp, [])

        # now define a replication task
        resp = self.monitor.couchServer.replicate(self.sourceUrl, self.targetUrl, sleepSecs=5)
        self.assertTrue(resp["ok"])
        time.sleep(1)
        # FIXME: this was supposed to be 1. To be investigated!!!
        self.assertEqual(len(self.monitor.getActiveTasks()), 0)

    def testGetSchedulerJobs(self):
        """
        Test the 'getSchedulerJobs' method, which is only meaningful
        for CouchDB version >= 3
        """
        resp = self.monitor.couchServer.getCouchWelcome()
        if resp["version"] == "1.6.1":
            return

        resp = self.monitor.getSchedulerJobs()
        self.assertEqual(resp["total_rows"], 0)
        self.assertEqual(resp["jobs"], [])

        # now define a replication task
        resp = self.monitor.couchServer.replicate(self.sourceUrl, self.targetUrl, sleepSecs=5)
        self.assertTrue(resp["ok"])
        # give it a couple of seconds for it to show up as active task
        time.sleep(1)
        resp = self.monitor.getSchedulerJobs()
        self.assertEqual(resp["total_rows"], 1)
        self.assertEqual(len(resp["jobs"]), 1)
        self.assertEqual(len(resp["jobs"]["database"]), "_replicator")

    def testGetSchedulerDocs(self):
        """
        Test the 'getSchedulerDocs' method, which is only meaningful
        for CouchDB version >= 3
        """
        resp = self.monitor.couchServer.getCouchWelcome()
        if resp["version"] == "1.6.1":
            return

        resp = self.monitor.getSchedulerDocs()
        self.assertEqual(resp["total_rows"], 0)
        self.assertEqual(resp["docs"], [])

        # now define a replication task
        resp = self.monitor.couchServer.replicate(self.sourceUrl, self.targetUrl, sleepSecs=5)
        self.assertTrue(resp["ok"])
        # give it a couple of seconds for it to show up as active task
        time.sleep(1)
        resp = self.monitor.getSchedulerDocs()
        self.assertEqual(resp["total_rows"], 1)
        self.assertEqual(len(resp["docs"]), 1)
        self.assertEqual(len(resp["docs"]["database"]), "_replicator")

    def testCheckCouchReplications(self):
        """Very basic tests for the 'checkCouchReplications' method"""
        resp = self.monitor.checkCouchReplications([])
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["error_message"], "")

    def testCheckReplicationState(self):
        """Very basic tests for the 'checkReplicationState' method"""
        resp = self.monitor.checkReplicationState()
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["error_message"], "")

    def testIsReplicationOK(self):
        """ Very basic test for the 'isReplicationOK' method """
        # define last update as of now - 15min
        myUpdateOn = int(time.time() - 1 * 60)
        checkPoint = 600000  # every 10min (in ms)
        testRepl = dict(updated_on=myUpdateOn, checkpoint_interval=checkPoint)
        resp = self.monitor.isReplicationOK(testRepl)
        self.assertTrue(resp)

        # now change last update to now - 2h to reproduce an error
        myUpdateOn = int(time.time() - 20 * 60)
        testRepl = dict(updated_on=myUpdateOn, checkpoint_interval=checkPoint)
        resp = self.monitor.isReplicationOK(testRepl)
        self.assertFalse(resp)


if __name__ == '__main__':
    unittest.main()
