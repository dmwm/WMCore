from builtins import range
import unittest
import os
import sys
from datetime import timedelta
from time import sleep

from nose.plugins.attrib import attr

from WMCore.Database.CMSCouch import RotatingDatabase, CouchServer, CouchNotFoundError


class RotatingDatabaseTest(unittest.TestCase):
    def setUp(self):
        self.couchURL = os.getenv("COUCHURL")
        self.server = CouchServer(self.couchURL)

        testname = self.id().split('.')[-1].lower()
        self.dbname = 'rotdb_unittest_%s' % testname
        self.arcname = 'rotdb_unittest_%s_archive' % testname
        self.seedname = 'rotdb_unittest_%s_seedcfg' % testname
        # set a long value for times, tests do operations explicitly
        self.timing = {'archive':timedelta(seconds=1), 'expire':timedelta(seconds=2)}

        self.db = RotatingDatabase(dbname = self.dbname, url = self.couchURL,
                                   archivename = self.arcname, timing = self.timing)

    def tearDown(self):
        """Delete all the test couchdb databases"""
        to_go = [db for db in self.server.listDatabases() if db.startswith('rotdb_unittest_')]
        for dbname in to_go:
            try:
                self.server.deleteDatabase(dbname)
            except CouchNotFoundError:
                # db has already gone
                pass

    def testRotate(self):
        """
        Test that rotation works
        """
        start_name = self.db.name
        self.db._rotate()
        end_name = self.db.name
        databases = [db for db in self.server.listDatabases() if db.startswith('rotdb_unittest_')]
        self.assertTrue(start_name in databases)
        self.assertTrue(end_name in databases)

    # This test repeatably inserts either 20 or 25 documents into couch.
    # Disabled until it's stable
    @attr("integration")
    def testArchive(self):
        """
        Test that archiving views works
        """
        dummy_view = {'_id':'_design/foo', 'language': 'javascript','views':{
                        'bar':{'map':"function(doc) {if (doc.foo) {emit(doc.int, 1);}}", 'reduce':'_sum'}
                        }
                    }
        archive_view = {'_id':'_design/foo', 'language': 'javascript','views':{
                        'bar':{'map':"function(doc) {emit(doc.key, doc.value);}", 'reduce':'_sum'}
                        }
                    }

        seed_db = self.server.connectDatabase(self.seedname)
        seed_db.commit(dummy_view)
        # Need to have the timing long enough so the data isn't archived by accident
        self.timing = {'archive':timedelta(seconds=1000), 'expire':timedelta(seconds=2000)}
        self.db = RotatingDatabase(dbname = self.dbname, url = self.couchURL, views=['foo/bar'],
                                archivename = self.arcname, timing = self.timing)
        self.db.archive_db.commitOne(archive_view)
        runs = 5
        docs = 5
        for run in range(runs):
            for i in range(docs):
                self.db.queue({'foo':'bar', 'int': i, 'run': run})
            self.db.commit()
            self.db._rotate()
        self.db._archive()
        view_result = self.db.archive_db.loadView('foo','bar')
        arch_sum = view_result['rows'][0]['value']
        self.assertEqual(arch_sum, runs * docs)

    def testExpire(self):
        """
        Test that expiring databases works
        """
        # rotate out the original db
        self.db._rotate()
        archived = self.db.archived_dbs()
        self.assertEqual(1, len(archived), 'test not starting from clean state, bail!')
        # Make sure the db has expired
        sleep(2)
        self.db._expire()
        self.assertEqual(0, len(self.db.archived_dbs()))
        self.assertFalse(archived[0] in self.server.listDatabases())

    @attr("integration")
    def testCycle(self):
        """
        Test that committing data to different databases happens
        This is a bit of a dodgy test - if timings go funny it will fail
        """
        self.timing = {'archive':timedelta(seconds=0.5), 'expire':timedelta(seconds=1)}
        self.db = RotatingDatabase(dbname = self.dbname, url = self.couchURL,
                                   archivename = self.arcname, timing = self.timing)
        my_name = self.db.name
        self.db.commit({'foo':'bar'})
        sleep(5)
        self.db.commit({'foo':'bar'})
        # the initial db should have expired by now
        self.db.commit({'foo':'bar'})
        self.assertFalse(my_name in self.server.listDatabases(), "")


if __name__ == "__main__":
    if len(sys.argv) >1 :
        suite = unittest.TestSuite()
        suite.addTest(RotatingDatabaseTest(sys.argv[1]))
        unittest.TextTestRunner().run(suite)
    else:
        unittest.main()
