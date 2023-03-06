"""
File       : MSPileupTasks_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/MSPileupReport.py module
"""

# system modules
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.DataStructs.MSPileupReport import MSPileupReport


class MSPileupReportTest(unittest.TestCase):
    """Unit test for MSPileupTasks module"""

    def testMSPileupReport(self):
        """
        Test MSPileup report functionality
        """
        mgr = MSPileupReport(autoCleanup=True)
        entry = 'test'
        task = 'tast'
        uuid = '123'
        mgr.addEntry(task, uuid, entry)

        # check documents functionality
        docs = mgr.getDocuments()
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]['entry'], entry)
        self.assertEqual(docs[0]['task'], task)
        self.assertEqual(docs[0]['uuid'], uuid)

        # check dictionary funtionality
        rdict = mgr.getReportByUuid()
        self.assertEqual(uuid in rdict, True)
        self.assertEqual(len(rdict[uuid]), 1)
        self.assertEqual(entry in rdict[uuid][0], True)
        self.assertEqual(uuid in rdict[uuid][0], True)
        self.assertEqual(task in rdict[uuid][0], True)

        # test purge functionality
        mgr = MSPileupReport(autoExpire=-1, autoCleanup=True)
        mgr.addEntry(task, uuid, entry)
        docs = mgr.getDocuments()
        self.assertEqual(len(docs), 0)


if __name__ == '__main__':
    unittest.main()
