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
            filesetName = None
            metadata = {}

          
            if inpDataset != None:
                filesetName = "/%s/%s/%s" % (inpDataset.primary, inpDataset.processed, inpDataset.tier)
                metadata['input_dataset'] = {}
                #hmmn, no recursive dictionary conversion for ConfigSection? pretty sure I wrote one somewhere...
                for key, val in inpDataset.dictionary_().items():
                    if val.__class__.__name__ == "ConfigSection":
                        metadata['input_dataset'][key] = val.dictionary_()
                    else:
                        metadata['input_dataset'][key] = val
            
            if inpStep != None:
                step = stepMap[inpStep]
                if step.stepType() != "CMSSW": continue
                metadata['input_step'] = {}
                metadata['input_step']['step_name'] = inpStep
                outputModule = task.data.input.outputModule
                metadata['input_step']['module_name'] = outputModule
                outModConfig = getattr(step.data.output.modules, outputModule, None)
                if outModConfig == None: continue
                filesetName = "%s/%s/%s" % (outModConfig.primaryDataset, outModConfig.processedDataset,
                                            outModConfig.dataTier)
                metadata['input_dataset'] = {}
                metadata['input_dataset'].update(outModConfig.dictionary_())

                #anything else to add here that might be useful?
          
            # create a fileset in the collection for each Task, add extra information
            # about the input dataset or step to the fileset
            if filesetName == None: continue
            fileset = CouchFileset(dataset = filesetName, url = self.url, database = self.database)
            fileset.setCollection(collection)

                            
            # this isnt doing things in bulk, which may explain turdmuching performance...
            fileset['task'] = t
            fileset['metadata'] = metadata
            fileset.create()
        
    @CouchUtils.connectToCouch
    def listDataCollections(self):
        """
        _listDataCollections_

        List the collections by type, since for data collections we are looking them up
        by request/workloadspec ID instead of owner

        """
        result = self.couchdb.loadView("ACDC", 'data_collections',
             {}, []
            )


        for row in result[u'rows']:
            ownerInfo = row[u'value'][u'owner']
            collId = row[u'value'][u'_id']
            owner = self.newOwner(ownerInfo[u'group'], ownerInfo[u'user'])
            coll = CouchCollection(collection_id = collId,
                                   database = self.database, url = self.url)
            coll.setOwner(owner)
            coll.get()
            yield coll
    
    def getDataCollection(self, collName):
        """
        _getDataCollection_
        
        Get a data collection by name
        """
        result = self.couchdb.loadView("ACDC", 'data_collections',
                 { 'startkey' : collName }, []
                )
        # somewhat flaky assumption that we get one row, need to add some safeguards on this
        row = result[u'rows'][0]
        ownerInfo =  row[u'value'][u'owner']
        collId =  row[u'value'][u'_id']
        owner = self.newOwner(ownerInfo[u'group'], ownerInfo[u'user'])
        coll = CouchCollection(collection_id = collId,
                               database = self.database, url = self.url)
        coll.setOwner(owner)
        coll.get()
        return coll
        
    def filesetsByTask(self, collection, taskName):
        """
        _filesetsByTask_
        
        Util to get filesets IDs by 
        """
        result = self.couchdb.loadView("ACDC", 'datacoll_filesets',
                 { 'startkey' : [collection['collection_id'], taskName], 'endkey': [collection['collection_id'], taskName] }, []
                )
        for row in result[u'rows']:
            doc = self.couchdb.document(row[u'value'])
            yield doc
        

        
    @CouchUtils.connectToCouch
    def failedJobs(self, *failedJobs):
        """
        _failedJobs_
        
        Given a list of failed jobs, sort them into Filesets and record them
        """
        collections = {}
        for job in failedJobs:
            taskName = job['task']
            workflow = job['workflow']
            if not collections.has_key(workflow):
                collections['workflow'] = self.getDataCollection(job['workflow'])
            coll = collections['workflow']
            for fileset in self.filesetsByTask(coll, taskName):
                cFileset = CouchFileset(database = self.database, url = self.url, fileset_id = fileset['_id'])
                cFileset.setCollection(coll)
                cFileset.add(*job['input_files'])
        
    
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
from WMCore.DataStructs.JobPackage import JobPackage

class DataCollectionInterfaceTests(unittest.TestCase):

    def setUp(self):
        self.databaseName = u"acdcdatacollinterfacetest"
        self.harness = CouchAppTestHarness(self.databaseName, 'http://evansde:Gr33nMan@127.0.0.1:5984')
        self.harness.create()
        self.harness.pushCouchapps("/Users/evansde/Documents/AptanaWorkspace/WMCORE/src/couchapps/GroupUser/")
        self.harness.pushCouchapps("/Users/evansde/Documents/AptanaWorkspace/WMCORE/src/couchapps/ACDC/")


        self.workloadF = "/Users/evansde/Documents/AptanaWorkspace/WMCORE/test/data/cmsdataops_100806_091657/WMSandbox/WMWorkload.pkl"
        self.workload = WMWorkloadHelper()
        self.workload.load(self.workloadF)
                
    def tearDown(self):
        
        self.harness.drop()
        pass
    
    def testA(self):
        
        dcs = DataCollectionService(url = self.harness.couchUrl, database = self.harness.dbName)
        
        dcs.createCollection(self.workload)
        

        
        pkg = JobPackage()
        pkg.load("/Users/evansde/Documents/AptanaWorkspace/WMCORE/test/data/cmsdataops_100806_091657/batch_2177-0/JobPackage.pkl")
        
        #for c in dcs.listDataCollections():
        #    print c, c.owner
        
        #c = dcs.getDataCollection('cmsdataops_100806_091657')
        #for f in dcs.listFilesets(c):
        #    print f
        
        
        #for f in dcs.filesetsByTask(c, pkg[2212]['task']):
        #    #print count 
        #    print f
        #   #count +=1
        
        #print pkg[2212]
        
        dcs.failedJobs(pkg[2212], pkg[2213])
        

if __name__ == '__main__':
    unittest.main()