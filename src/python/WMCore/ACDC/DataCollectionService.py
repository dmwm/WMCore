343#!/usr/bin/env python
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
from WMCore.WMException      import WMException
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run  import Run

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
        userName  = getattr(wmSpec.data.owner, 'name', None)
        groupName = getattr(wmSpec.data.owner, 'group', None)
        
        if userName == None:
            msg = "WMSpec does not contain an owner.name parameter"
            raise RuntimeError(msg)
        if groupName == None:
            msg = "WMSpec does not contain an owner.group parameter"
            raise RuntimeError(msg)
            
        
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

            filesetName = t
            if inpDataset != None:
                metadata['input_dataset'] = {}
                #hmmn, no recursive dictionary conversion for ConfigSection? pretty sure I wrote one somewhere...
                for key, val in inpDataset.dictionary_().items():
                    if val.__class__.__name__ == "ConfigSection":
                        metadata['input_dataset'][key] = val.dictionary_()
                    else:
                        metadata['input_dataset'][key] = val
            
            if inpStep != None:
                step = stepMap[inpStep]
                if step.stepType() != "CMSSW":
                    continue
                metadata['input_step'] = {}
                metadata['input_step']['step_name'] = inpStep
                outputModule = task.data.input.outputModule
                metadata['input_step']['module_name'] = outputModule
                outModConfig = getattr(step.data.output.modules, outputModule, None)
                if outModConfig == None:
                    continue
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

        return collection
        
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
            
    @CouchUtils.connectToCouch
    def getDataCollection(self, collName):
        """
        _getDataCollection_
        
        Get a data collection by name
        """
        result = self.couchdb.loadView("ACDC", 'data_collections',
                                       {'startkey': collName,
                                        'endkey': collName}, [])

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
            coll = CouchFileset(_id = row['value'], database = self.database, url = self.url)
            coll.get()
            yield coll
        

        
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
                logging.debug("Inserting fileset into ACDC with failed jobs: %s" % fileset)
                cFileset = CouchFileset(database = self.database, url = self.url, _id = fileset[u'_id'])
                cFileset.setCollection(coll)
                cFileset['task'] = taskName
                cFileset.add(files = job['input_files'], mask = job['mask'])


    @CouchUtils.connectToCouch
    def chunkFileset(self, collection, taskName, chunkSize = 100):
        """
        _chunkFileset_

        Split all of the fileset in a given collection/task into chunks.  This
        will return a list of dictionaries that contain the offset into the
        fileset and a summary of files/events/lumis that are in the fileset
        chunk.
        """
        chunks        = []
        collection_id = collection['collection_id']
        results = self.couchdb.loadView("ACDC", "fileset_metadata",
                                        {"startkey": [collection_id, taskName],
                                         "endkey": [collection_id, taskName, {}]}, [])

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for row in results["rows"]:
            if currentLocation == None:
                currentLocation = row["key"][2]
            if numFilesInBlock == chunkSize or currentLocation != row["key"][2]:
                chunks.append({"offset": totalFiles, "files": numFilesInBlock,
                               "events": numEventsInBlock, "lumis": numLumisInBlock,
                               "locations": currentLocation})
                totalFiles += numFilesInBlock
                currentLocation = row["key"][2]
                numFilesInBlock = 0
                numLumisInBlock = 0
                numEventsInBlock = 0

            numFilesInBlock += 1
            numLumisInBlock += row["value"]["lumis"]
            numEventsInBlock += row["value"]["events"]
            
        if numFilesInBlock > 0:
            chunks.append({"offset": totalFiles, "files": numFilesInBlock,
                           "events": numEventsInBlock, "lumis": numLumisInBlock,
                           "locations": currentLocation})
        return chunks

    @CouchUtils.connectToCouch
    def getChunkInfo(self, collection, taskName, chunkOffset, chunkSize):
        """
        _getChunkInfo_

        Retrieve metadata for a particular chunk.
        """
        collection_id = collection["collection_id"]
        results = self.couchdb.loadView("ACDC", "fileset_metadata",
                                        {"startkey": [collection_id, taskName],
                                         "endkey": [collection_id, taskName, {}],
                                         "skip": chunkOffset,
                                         "limit": chunkSize}, [])

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for row in results["rows"]:
            if currentLocation == None:
                currentLocation = row["key"][2]

            numFilesInBlock += 1
            numLumisInBlock += row["value"]["lumis"]
            numEventsInBlock += row["value"]["events"]
            
        return {"offset": totalFiles, "files": numFilesInBlock,
                "events": numEventsInBlock, "lumis": numLumisInBlock,
                "locations": currentLocation}
    
    @CouchUtils.connectToCouch
    def getChunkFiles(self, collection, taskName, chunkOffset, chunkSize = 100):
        """
        _getChunkFiles_

        Retrieve a chunk of files from the given collection and task.
        """
        collection_id = collection['collection_id']
        chunkFiles = []
        result = self.couchdb.loadView("ACDC", "fileset_files",
                                       {"startkey": [collection_id, taskName],
                                        "endkey": [collection_id, taskName, {}],
                                        "limit": chunkSize,
                                        "skip": chunkOffset,
                                        }, [])

        for row in result["rows"]:
            resultRow = row['value']
            newFile = File(lfn = resultRow["lfn"], size = resultRow["size"],
                           events = resultRow["events"], parents = set(resultRow["parents"]),
                           locations = set(resultRow["locations"]), merged = resultRow["merged"])
            for run in resultRow["runs"]:
                newRun = Run(run["run_number"])
                newRun.extend(run["lumis"])
                newFile.addRun(newRun)
                
            chunkFiles.append(newFile)

        return chunkFiles

    @CouchUtils.connectToCouch
    def getLumiWhitelist(self, collectionID, taskName):
        """
        _getLumiWhitelist_

        Query ACDC for all of the files in the given collection and task.
        Generate a run and lumi whitelist for the given files with the following
        format:
          {"run1": [[lumi1, lumi4], [lumi6, lumi10]],
           "run3": [lumi5, lumi10]}

        Note that the run numbers are strings.
        """
        results = self.couchdb.loadView("ACDC", "fileset_files",
                                        {"startkey": [collectionID, taskName],
                                         "endkey": [collectionID, taskName, {}]}, [])

        allRuns = {}
        whiteList = {}

        for result in results["rows"]:
            for run in result["value"]["runs"]:
                if not allRuns.has_key(run["run_number"]):
                    allRuns[run["run_number"]] = []
                allRuns[run["run_number"]].extend(run["lumis"])

        for run in allRuns.keys():
            lumis = []
            lumis.extend(set(allRuns[run]))
            lumis.sort()

            whiteList[str(run)] = []
            lastLumi = None
            currentSet = None

            while len(lumis) > 0:
                currentLumi = lumis.pop(0)
                if currentLumi - 1 != lastLumi:
                    if currentSet == None:
                        currentSet = [currentLumi]
                    else:
                        currentSet.append(lastLumi)
                        whiteList[str(run)].append(currentSet)
                        currentSet = [currentLumi]

                lastLumi = currentLumi

            currentSet.append(lastLumi)
            whiteList[str(run)].append(currentSet)
            
        return whiteList
