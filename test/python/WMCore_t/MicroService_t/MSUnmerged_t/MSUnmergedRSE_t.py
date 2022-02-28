"""
Unit tests for MicorService/MSUnmerged/MSUnmergedRSE.py module

"""
from __future__ import division, print_function

import json
import os
import unittest
import mongomock

from pymongo import IndexModel

from WMCore.MicroService.MSUnmerged.MSUnmerged import MSUnmerged, MSUnmergedRSE


class MSUnmergedRSETest(unittest.TestCase):
    """ Unit test for MSUnmergedRSE module """

    def setUp(self):
        """ init test class """

        self.maxDiff = None

        msUnmergedIndex = IndexModel('name', unique=True)
        self.msUnmergedColl = mongomock.MongoClient().msUnmergedDB.msUnmergedColl
        self.msUnmergedColl.create_indexes([msUnmergedIndex])

        self.expectedRSE = {'name': 'T2_US_Wisconsin',
                            'pfnPrefix': None,
                            'isClean': False,
                            "rucioConMonStatus": None,
                            'timestamps': {'rseConsStatTime': 0.0,
                                           'prevStartTime': 0.0,
                                           'startTime': 0.0,
                                           'prevEndTime': 0.0,
                                           'endTime': 0.0},
                            'counters': {'totalNumFiles': 0,
                                         'totalNumDirs': 0,
                                         'dirsToDelete': 0,
                                         'filesToDelete': 0,
                                         'filesDeletedSuccess': 0,
                                         'filesDeletedFail': 0,
                                         'dirsDeletedSuccess': 0,
                                         'dirsDeletedFail': 0,
                                         'gfalErrors': {}},
                            'files': {'allUnmerged': [],
                                      'toDelete': {},
                                      'protected': {},
                                      'deletedSuccess': set(),
                                      'deletedFail': set()},
                            'dirs': {'allUnmerged': set(),
                                     'toDelete': set(),
                                     'protected': set(),
                                     'deletedSuccess': set(),
                                     'deletedFail': set()}}

        super(MSUnmergedRSETest, self).setUp()

    def testCreateRSE(self):
        rse = MSUnmergedRSE('T2_US_Wisconsin')
        self.assertDictEqual(rse, self.expectedRSE)

    def testRSEReadWrite(self):
        rse = MSUnmergedRSE('T2_US_Wisconsin')
        rse.writeRSEToMongoDB(self.msUnmergedColl)
        rse.readRSEFromMongoDB(self.msUnmergedColl)
        rse.pop('_id', None)
        self.assertDictEqual(rse, self.expectedRSE)
