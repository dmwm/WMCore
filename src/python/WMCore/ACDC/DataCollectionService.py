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
import logging

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
import WMCore.ACDC.CouchUtils as CouchUtils
import WMCore.ACDC.CollectionTypes as CollectionTypes

from WMCore.WMSpec.Utilities import stepIdentifier

from WMCore.WMException import WMException

class ACDCDCSException(WMException):
    """
    Yet another dummy variable class

    """

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
    def __init__(self, url, database, **opts):
        CouchService.__init__(self, url = url,
                              database = database,
                              **opts)
        
    @CouchUtils.connectToCouch
    def createCollection(self, wmSpec):
        """
        _createCollection_
        
        Create a DataCollection from the wmSpec instance provided
        """
        userName  = getattr(wmSpec.data.request.schema, 'Requestor', None)
        groupName = getattr(wmSpec.data.request.schema, 'Group', None)
        
        user = self.newOwner(groupName, userName)
        collection = CouchCollection(
            name = wmSpec.name(), collection_type = CollectionTypes.DataCollection,
            url = self.url, database = self.database
            )
        collection.setOwner(user)
        collection.create()

        stepMap = StepMap()
        stepMap.fill(wmSpec)

        filesetsMade = []
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
            if filesetName in filesetsMade: continue
            fileset = CouchFileset(dataset = filesetName, url = self.url, database = self.database)
            fileset.setCollection(collection)

                            
            # this isnt doing things in bulk, which may explain turdmuching performance...
            fileset['task'] = t
            fileset['metadata'] = metadata
            fileset.create()
            filesetsMade.append(filesetName)
        
    @CouchUtils.connectToCouch
    def yieldDataCollections(self):
        """
        _yieldDataCollections_

        List the collections by type, since for data collections we are looking them up
        by request/workloadspec ID instead of owner

        This is meant to be done as an iterative loop, i.e.,

        for collection in self.yieldDataCollections():
          collection.doSomething()

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


    def listDataCollections(self):
        """
        _listDataCollections_

        Use the yieldDataCollections() function to provide
        a list of all data collections
        """

        return [x for x in self.yieldDataCollections()]
            
    
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
    def failedJobs(self, failedJobs):
        """
        _failedJobs_
        
        Given a list of failed jobs, sort them into Filesets and record them

        NOTE: jobs must have a non-standard 'task' and 'workflow' key assigned
        to them.
        """
        collections = {}
        for job in failedJobs:
            try:
                taskName = job['task']
                workflow = job['workflow']
            except KeyError, ex:
                msg =  "Missing required, non-standard key %s in job in ACDC.DataCollectionService" % (str(ex))
                logging.error(msg)
                raise ACDCDCSException(msg)
            if not collections.has_key(workflow):
                collections['workflow'] = self.getDataCollection(job['workflow'])
            coll = collections['workflow']
            for fileset in self.filesetsByTask(coll, taskName):
                cFileset = CouchFileset(database = self.database, url = self.url, fileset_id = fileset['_id'])
                cFileset.setCollection(coll)
                cFileset.add(*job['input_files'])
        
    


