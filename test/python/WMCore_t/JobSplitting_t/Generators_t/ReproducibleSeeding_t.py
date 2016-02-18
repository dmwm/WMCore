#!/usr/bin/env python
# encoding: utf-8
"""
ReproducibleSeeding_t.py

Created by Dave Evans on 2010-08-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.JobSplitting.Generators.ReproducibleSeeding import ReproducibleSeeding
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.DataStructs.Job import Job
from WMCore.Database.CMSCouch import Database, Document



class ReproducibleSeedingTests(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInitCouchApp("ReproducibleSeedingTest")
        self.testInit.setupCouch("seeding_config_cache", "GroupUser", "ConfigCache")
        self.database = Database(self.testInit.couchDbName, self.testInit.couchUrl)

        self.documentId = None

    def tearDown(self):
        self.testInit.tearDownCouch()
        return


    def testA(self):
        """instantiate"""

        document = Document()
        document[u'pset_tweak_details'] = {}
        document[u'pset_tweak_details'][u'process'] = {}
        document[u'pset_tweak_details'][u'process'][u'RandomNumberGeneratorService'] = {}
        document[u'pset_tweak_details'][u'process'][u'RandomNumberGeneratorService'][u'seed1'] = {}
        document[u'pset_tweak_details'][u'process'][u'RandomNumberGeneratorService'][u'seed2'] = {}
        document[u'pset_tweak_details'][u'process'][u'RandomNumberGeneratorService'][u'seed3'] = {}

        document = self.database.commitOne(document)[0]
        seeder = ReproducibleSeeding(CouchUrl = self.testInit.couchUrl,
                                     CouchDBName = self.testInit.couchDbName,
                                     ConfigCacheDoc = document[u'id'])

        job = Job("testjob")
        seeder(job)
        baggage = job.getBaggage()
        seed1 = getattr(baggage.process.RandomNumberGeneratorService, "seed1", None)
        self.assertTrue(seed1 != None)


if __name__ == '__main__':
    unittest.main()
