#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueuePopulator.py,v 1.1 2010/01/20 22:06:38 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import os
import shutil
from copy import deepcopy, copy

from WMQuality.TestInit import TestInit
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask

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
    

def createProductionSpec(fileName='testworkflow.spec'):
    specFile =  os.path.join(os.getcwd(), fileName)   
    # Basic production Spec
    spec = BasicProductionWorkload
    spec.setSpecUrl(specFile)
    spec.save(spec.specUrl())
    
    return specFile

def createProcessingSpec(fileName='testProcessing.spec'):
    specFile =  os.path.join(os.getcwd(), fileName)   
    # Basic production Spec
    spec = Tier1ReRecoWorkload
    spec.setSpecUrl(specFile)
    spec.save(spec.specUrl())
    
    return specFile


def getGlobalQueue(dbi, **kwargs):
    gQueue = globalQueue(dbi = dbi, **kwargs)
    
        # setup Mock DBS and PhEDEx
    inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
    dataset = "/%s/%s/%s" % (inputDataset.primary,
                             inputDataset.processed,
                             inputDataset.tier)
    mockDBS = MockDBSReader('http://example.com', dataset)
    gQueue.dbsHelpers['http://example.com'] = mockDBS
    gQueue.dbsHelpers['http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'] = mockDBS
    gQueue.phedexService = MockPhedexService(dataset)
    
    return gQueue
