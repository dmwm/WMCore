#!/usr/bin/env python
"""
This unit test isn't really a unit test, more an explaination of how to use the
CMSCouch library to work with CouchDB. It currently assumes you have a running
CouchDB instance, and is not going to work in an automated way just yet - we'll
need to add Couch as an external, include it in start up scripts etc.
"""

from WMCore.Database.CMSCouch import CouchServer, Document, Database, CouchInternalServerError, CouchNotFoundError
import random
import unittest
import os
import hashlib
import base64
import sys

class CMSCouchTest(unittest.TestCase):
    test_counter = 0
    def setUp(self):
        # Make an instance of the server
        self.server = CouchServer(os.getenv("COUCHURL", 'http://admin:password@localhost:5984'))
        self.testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        dbname = 'cmscouch_unittest_%s' % self.testname.lower()

        if dbname in self.server.listDatabases():
            self.server.deleteDatabase(dbname)

        self.server.createDatabase(dbname)
        self.db = self.server.connectDatabase(dbname)

    def tearDown(self):
        if sys.exc_info()[0] == None:
            # This test has passed, clean up after it
            dbname = 'cmscouch_unittest_%s' % self.testname.lower()
            self.server.deleteDatabase(dbname)

    def testCommitOne(self):
        # Can I commit one dict
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc, returndocs=True)[0]['id']
        # What about a Document
        doc = Document(inputDict = doc)
        id = self.db.commitOne(doc, returndocs=True)[0]['id']

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
        id = self.db.commitOne(doc, returndocs=True)[0]['id']
        self.assertEqual(1, len(self.db.allDocs()['rows']))
        self.db.commit()
        self.assertEqual(6, len(self.db.allDocs()['rows']))

    def testTimeStamping(self):
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc, timestamp=True, returndocs=True)[0]['id']
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

    def testWriteReadDocNoID(self):
        doc = {}

    def testReplicate(self):
        repl_db = self.server.connectDatabase(self.db.name + 'repl')

        doc_id = self.db.commitOne({'foo':123}, timestamp=True, returndocs=True)[0]['id']
        doc_v1 = self.db.document(doc_id)

        #replicate
        self.server.replicate(self.db.name, repl_db.name)

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
        self.assertEquals(d['foo'], d_from_db['foo'])
        self.assertEquals(d['timestamp'], d_from_db['timestamp'])

    def testAttachments(self):
        """
        Test uploading attachments with and without checksumming
        """
        doc = self.db.commitOne({'foo':'bar'}, timestamp=True, returndocs=True)[0]
        attachment1 = "Hello"
        attachment2 = "How are you today?"
        attachment3 = "I'm very well, thanks for asking"
        attachment4 = "Lovely weather we're having"
        attachment5 = "Goodbye"
        keyhash = hashlib.md5()
        keyhash.update(attachment5)
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

        doc_id = self.db.commitOne({'foo':123}, timestamp=True, returndocs=True)[0]['id']
        doc_v1 = self.db.document(doc_id)

        #replicate
        self.server.replicate(self.db.name, repl_db.name)

        doc_v2 = self.db.document(doc_id)
        doc_v2['bar'] = 456
        doc_id_rev2 = self.db.commitOne(doc_v2, returndocs=True)[0]
        doc_v2 = self.db.document(doc_id)

        #now update the replica
        conflict_doc = repl_db.document(doc_id)
        conflict_doc['bar'] = 101112
        repl_db.commitOne(conflict_doc)

        #replicate, creating the conflict
        self.server.replicate(self.db.name, repl_db.name)
        conflict_view = {'map':"function(doc) {if(doc._conflicts) {emit(doc._conflicts, null);}}"}
        data = repl_db.post('/%s/_temp_view' % repl_db.name, conflict_view)

        # Should have one conflict in the repl database
        self.assertEquals(data['total_rows'], 1)
        # Should have no conflicts in the source database
        self.assertEquals(self.db.post('/%s/_temp_view' % self.db.name, conflict_view)['total_rows'], 0)
        self.assertTrue(repl_db.documentExists(data['rows'][0]['id'], rev=data['rows'][0]['key'][0]))

        repl_db.delete_doc(data['rows'][0]['id'], rev=data['rows'][0]['key'][0])
        data = repl_db.post('/%s/_temp_view' % repl_db.name, conflict_view)

        self.assertEquals(data['total_rows'], 0)
        self.server.deleteDatabase(repl_db.name)

        #update it again
        doc_v3 = self.db.document(doc_id)
        doc_v3['baz'] = 789
        doc_id_rev3 = self.db.commitOne(doc_v3, returndocs=True)[0]
        doc_v3 = self.db.document(doc_id)

        #test that I can pull out an old revision
        doc_v1_test = self.db.document(doc_id, rev=doc_v1['_rev'])
        self.assertEquals(doc_v1, doc_v1_test)

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
        self.assertEqual(answer[0]['error'], 'conflict')
        self.assertEqual(answer[1]['error'], 'conflict')

        # all_or_nothing mode ignores conflicts
        self.db.queue(Document(id = "2", inputDict = doc))
        self.db.queue(Document(id = "2", inputDict = {'foo':1234, 'bar':456}))
        answer = self.db.commit(all_or_nothing = True)
        self.assertEqual(2, len(answer))
        self.assertEqual(answer[0].get('error'), None)
        self.assertEqual(answer[0].get('error'), None)
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

        self.db.queue(Document(id = "2", inputDict = {'foo':5, 'bar':6}))
        answer = self.db.commit(callback = callback)
        self.assertEqual(1, len(answer))
        self.assertEqual(answer[0].get('error'), None)
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
        self.assertEquals("bumped it!", self.db.updateDocument(doc_id, 'foo', 'bump-counter'))

        self.assertEquals(1, self.db.document(doc_id)['counter'])


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
        self.assertEquals(3, len(self.db.allDocs()['rows']))
        self.assertEquals(2, len(self.db.allDocs({'startkey': "2"})['rows']))
        self.assertEquals(2, len(self.db.allDocs(keys = ["1", "3"])['rows']))
        self.assertEquals(1, len(self.db.allDocs({'limit':1}, ["1", "3"])['rows']))
        self.assertEquals(True, self.db.allDocs(keys = ["1", "4"])['rows'][1].has_key('error'))

if __name__ == "__main__":
    if len(sys.argv) >1 :
        suite = unittest.TestSuite()
        suite.addTest(CMSCouchTest(sys.argv[1]))
        unittest.TextTestRunner().run(suite)
    else:
        unittest.main()
