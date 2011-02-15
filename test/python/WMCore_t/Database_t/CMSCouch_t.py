#!/usr/bin/env python
"""
This unit test isn't really a unit test, more an explaination of how to use the
CMSCouch library to work with CouchDB. It currently assumes you have a running
CouchDB instance, and is not going to work in an automated way just yet - we'll
need to add Couch as an external, include it in start up scripts etc.
"""

from WMCore.Database.CMSCouch import CouchServer, Document, Database, CouchInternalServerError
import random
import unittest
import os
import hashlib
import base64

class CMSCouchTest(unittest.TestCase):
    test_counter = 0
    def setUp(self):
        # Make an instance of the server
        self.server = CouchServer(os.getenv("COUCHURL", 'http://admin:password@localhost:5984'))
        testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        dbname = 'cmscouch_unittest_%s' % testname.lower()

        if dbname in self.server.listDatabases():
            self.server.deleteDatabase(dbname)

        self.server.createDatabase(dbname)
        self.db = self.server.connectDatabase(dbname)

    def tearDown(self):
        if self._exc_info()[0] == None:
            # This test has passed, clean up after it
            testname = self.id().split('.')[-1]
            dbname = 'cmscouch_unittest_%s' % testname.lower()
            self.server.deleteDatabase(dbname)

    def testCommitOne(self):
        # Can I commit one dict
        doc = {'foo':123, 'bar':456}
        id = self.db.commitOne(doc, returndocs=True)[0]['id']
        # What about a Document
        doc = Document(dict=doc)
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
        doc = Document(dict=doc)
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

        db = Database(db_name)
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

if __name__ == "__main__":
    unittest.main()
