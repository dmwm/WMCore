from WMCore.WMSpec.Persistency import PersistencyHelper
import unittest
from WMCore.WMSpec.WMStep import WMStep, makeWMStep


class PersistencyTest(unittest.TestCase):

    def testSplitUrl(self):
        helper = PersistencyHelper()
        url = 'https://cmsreqmgr.cern.ch/couchdb/mydb/doc/spec'
        base, dbname, doc = helper.splitCouchUrl(url)
        self.assertEqual(base, 'https://cmsreqmgr.cern.ch/couchdb')
        self.assertEqual(dbname, 'mydb')
        self.assertEqual(doc, 'doc/spec')

        url = 'http://localhost:5984/mydb/doc/spec'
        base, dbname, doc = helper.splitCouchUrl(url)
        self.assertEqual(base, 'http://localhost:5984')
        self.assertEqual(dbname, 'mydb')
        self.assertEqual(doc, 'doc/spec')


if __name__ == '__main__':
    unittest.main()
