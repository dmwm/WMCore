#!/usr/bin/env python
# encoding: utf-8
"""
ReproducibleSeeding.py

Created by Dave Evans on 2010-08-30.
Copyright (c) 2010 Fermilab. All rights reserved.

Seeding algorithm that generates job seeds at splitting time and saves them as part of the job itself, so that
the seeds are the same every time the job gets run (used for RelVal)

List of seeds is retrieved from the ConfigCache PSet Tweak

"""

import sys
import os
import unittest

from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Configuration import ConfigSection

import random
from random import SystemRandom
_inst  = SystemRandom()

#  //
# // Max bit mask size for 32 bit integers
#//  This is required to be max 900M for CMSSW
_MAXINT = 900000000
newSeed = lambda : _inst.randint(1, _MAXINT)


class ReproducibleSeeding(GeneratorInterface):
    def __init__(self, **options):
        GeneratorInterface.__init__(self, **options)
        self.couchUrl = options.get("CouchUrl")
        self.couchDBName = options.get("CouchDBName")   
        self.couchConfigDoc = options.get("ConfigCacheDoc")   
        
        config = ConfigSection("ConfigCache")
        config.section_("CoreDatabase")
        config.CoreDatabase.couchurl = self.couchUrl
        confCache = ConfigCache(config = config, couchDBName= self.couchDBName, id = self.couchConfigDoc)
        confCache.load()
        seeds = confCache.document[u'pset_tweak_details'][u'process'][u'RandomNumberGeneratorService']
        self.seedTable = []
        for k in seeds.keys():
            if k == u"parameters_" : continue
            self.seedTable.append("process.RandomNumberGeneratorService.%s.initialSeed" % k)
                
            
    def __call__(self, wmbsJob):
        wmbsJob.addBaggageParameter("seeding", self.__class__.__name__)

        for seed in self.seedTable:
            wmbsJob.addBaggageParameter(seed, newSeed())



from WMCore.DataStructs.Job import Job

class ReproducibleSeedingTests(unittest.TestCase):
    def setUp(self):
        pass

    def testA(self):
                
        repro = ReproducibleSeeding(
            CouchUrl = "http://dmwmwriter:PASSWORD@localhost:5986",
            CouchDBName = "config_cache1",
            ConfigCacheDoc = "a3c2f9c1d2231060b1de9dafe7d5b8f2"
        )
        
        job = Job("Job1")
        
        repro(job)
        
        
        

if __name__ == '__main__':
    unittest.main()