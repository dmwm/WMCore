#!/usr/bin/env python
# encoding: utf-8
"""
DataCollectionInterface.py

Created by Dave Evans on 2010-07-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
import WMCore.ACDC.CouchUtils as CouchUtils
import WMCore.ACDC.CollectionTypes as CollectionTypes

from WMCore.WMSpec.Utilities import stepIdentifier

class StepMap(dict):
    def __init__(self):
        dict.__init__(self)
        self.tasks = {}
    def fill(self, wmSpec):
        """
        _fill_
        
        """
        for t in wmSpec.listAllTaskPathNames():
            task = wmSpec.getTaskByPath(t)
            self.tasks[t] = task
            for s in task.listAllStepNames():
                step = task.getStepHelper(s)
                stepId = stepIdentifier(step)
                self[stepId] = step 
        
            
            

class DataCollectionService(CouchService):
    def __init__(self, **opts):
        CouchService.__init__(self, **opts)
        
    @CouchUtils.connectToCouch
    def createCollection(self, wmSpec):
        """
        _createCollection_
        
        Create a DataCollection from the wmSpec instance provided
        """
        groupName = getattr(wmSpec.data.request.schema, 'Requestor', None)
        userName = getattr(wmSpec.data.request.schema, 'Group', None)
        
        user = self.newOwner(groupName, userName)
        collection = CouchCollection(
            name = wmSpec.name(), collection_type = CollectionTypes.DataCollection,
            url = self.url, database = self.database
            )
        collection.setOwner(user)
        collection.create()

        stepMap = StepMap()
        stepMap.fill(wmSpec)

        
        for t in wmSpec.listAllTaskPathNames():
            task = wmSpec.getTaskByPath(t)
            inpDataset = getattr(task.data.input, "dataset", None)
            inpStep = getattr(task.data.input, "inputStep", None)
            
            # create a fileset in the collection for each Task, add extra information
            # about the input dataset or step to the fileset
            fileset = CouchFileset(name = t, url = self.url, database = self.database)
            fileset.setCollection(collection)
            metadata = {}

            if inpDataset != None:
                metadata['input_dataset'] = {}
                #hmmn, no recursive dictionary conversion for ConfigSection? pretty sure I wrote one somewhere...
                for key, val in inpDataset.dictionary_().items():
                    if val.__class__.__name__ == "ConfigSection":
                        metadata['input_dataset'][key] = val.dictionary_()
                    else:
                        metadata['input_dataset'][key] = val
                fileset['metadata'] = metadata
            if inpStep != None:
                step = stepMap[inpStep]
                if step.stepType() != "CMSSW": continue
                metadata['input_step'] = {}
                metadata['input_step']['step_name'] = inpStep
                #anything else to add here that might be useful?
                                    
            # this isnt doing things in bulk, which may explain turdmuching performance...
            fileset.create()
        
    @CouchUtils.connectToCouch
    def failedJobs(self, *failedJobs):
        """
        _failedJobs_
        
        Given a list of failed jobs, sort them into Filesets and record them
        """
        pass
    
from couchapp.commands import push as couchapppush
from couchapp.config import Config
from WMCore.Database.CMSCouch import CouchServer


class CouchAppTestHarness:
    
    def __init__(self, dbName, couchUrl = None):
        self.couchUrl = os.environ.get("COUCHURL", couchUrl)
        self.dbName = dbName
        if self.couchUrl == None:
            msg = "COUCHRURL env var not set..."
            raise RuntimeError, msg
        self.couchServer = CouchServer(self.couchUrl)
        self.couchappConfig = Config()
        
        
    def create(self):
        """create couch db instance"""
        if self.dbName in self.couchServer.listDatabases():
            msg = "Database already exists in couch instance. bailing..."
            raise RuntimeError, msg

        self.couchServer.createDatabase(self.dbName)
        
    def drop(self):
        """blow away the couch db instance"""
        self.couchServer.deleteDatabase(self.dbName)

    def pushCouchapps(self, *couchappdirs):
        """
        push a list of couchapps to the database
        """
        for couchappdir in  couchappdirs:
            couchapppush(self.couchappConfig, couchappdir, "%s/%s" % (self.couchUrl, self.dbName))
        



from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

class DataCollectionInterfaceTests(unittest.TestCase):

    def setUp(self):
        self.databaseName = u"acdcdatacollinterfacetest"
        self.harness = CouchAppTestHarness(self.databaseName, 'http://USER:PASS@127.0.0.1:5984')
        self.harness.create()
        self.harness.pushCouchapps("/Users/evansde/Documents/AptanaWorkspace/WMCORE/src/couchapps/GroupUser/")
        self.harness.pushCouchapps("/Users/evansde/Documents/AptanaWorkspace/WMCORE/src/couchapps/ACDC/")


        self.workloadF = "/Users/evansde/Documents/AptanaWorkspace/WMCORE/test/data/cmsdataops_100806_091657/WMSandbox/WMWorkload.pkl"
        self.workload = WMWorkloadHelper()
        self.workload.load(self.workloadF)
                
    def tearDown(self):
        
        self.harness.drop()

        
    def testA(self):
        
        dcs = DataCollectionService(url = self.harness.couchUrl, database = self.harness.dbName)
        
        dcs.createCollection(self.workload)

if __name__ == '__main__':
    unittest.main()