#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueuePopulator.py,v 1.1 2009/12/16 20:44:56 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import os
import shutil
from copy import deepcopy, copy

from WMQuality.TestInit import TestInit
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask

from WorkQueueTestCase import WorkQueueTestCase

from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload import workload as MultiTaskProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workingDir
shutil.rmtree(workingDir, ignore_errors = True)
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
from WMCore_t.WorkQueue_t.MockPhedexService import MockPhedexService

testInit = TestInit(__file__)
testInit.setLogging() # logLevel = logging.SQLDEBUG
testInit.setDatabaseConnection()

specFile = os.path.join(os.getcwd(), 'testworkflow.spec')
processingSpecFile = os.path.join(os.getcwd(), 'testProcessing.spec')
blacklistSpecFile = os.path.join(os.getcwd(), 'testBlacklist.spec')
whitelistSpecFile = os.path.join(os.getcwd(), 'testWhitelist.spec')
    
def populateGlobalWorkQueue():
    
    
    testInit.setSchema(customModules = ["WMCore.WorkQueue.Database"],
                                useDefault = False)
    # Basic production Spec
    spec = BasicProductionWorkload
    spec.setSpecUrl(specFile)
    spec.save(spec.specUrl())

    # Sample Tier1 ReReco spec
    processingSpec = Tier1ReRecoWorkload
    processingSpec.setSpecUrl(processingSpecFile)
    processingSpec.save(processingSpec.specUrl())

    # ReReco spec with blacklist
    blacklistSpec = deepcopy(processingSpec)
    blacklistSpec.setSpecUrl(blacklistSpecFile)
    blacklistSpec.taskIterator().next().data.constraints.sites.blacklist = ['SiteA']
    blacklistSpec.data._internal_name = 'blacklistSpec'
    blacklistSpec.save(blacklistSpec.specUrl())

    # ReReco spec with whitelist
    whitelistSpec = deepcopy(processingSpec)
    whitelistSpec.setSpecUrl(whitelistSpecFile)
    whitelistSpec.taskIterator().next().data.constraints.sites.whitelist = ['SiteB']
    blacklistSpec.data._internal_name = 'whitelistlistSpec'
    whitelistSpec.save(whitelistSpec.specUrl())

    # Create queues
    gQueue = globalQueue(CacheDir = 'global',
                                   NegotiationTimeout = 0,
                                   QueueURL = 'global.example.com')

    
    # setup Mock DBS and PhEDEx
    inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
    dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
    mockDBS = MockDBSReader('http://example.com', dataset)
    gQueue.dbsHelpers['http://example.com'] = mockDBS
    gQueue.dbsHelpers['http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'] = mockDBS
    gQueue.phedexService = MockPhedexService(dataset)
    
    # Queue Work & check accepted
    gQueue.queueWork(specFile)
    
    # Queue Work & check accepted
    gQueue.queueWork(processingSpecFile)
    
def cleanUpGlobalWorkQueue():
    
    testInit.clearDatabase()
    
    for f in (specFile, processingSpecFile,
              blacklistSpecFile, whitelistSpecFile):
        try:
            os.unlink(f)
        except OSError:
            pass

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) != 2:
        print "Need to specify p for popluate or c for clean up"  
    elif sys.argv[1] == 'p':
        print "Populate global work queue"
        populateGlobalWorkQueue()
    elif sys.argv[1] == 'c':
        print "Cleanup global work queue"
        cleanUpGlobalWorkQueue()
    else:
        print "Wrong argument"
# pylint: enable-msg=W0613,R0201