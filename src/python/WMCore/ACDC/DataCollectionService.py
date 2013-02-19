#!/usr/bin/env python
# encoding: utf-8
"""
DataCollectionInterface.py

Created by Dave Evans on 2010-07-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import logging

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset

import WMCore.Database.CouchUtils  as CouchUtils
import WMCore.ACDC.CollectionTypes as CollectionTypes

from WMCore.WMException      import WMException
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run  import Run

class ACDCDCSException(WMException):
    """
    Yet another dummy variable class

    """

class DataCollectionService(CouchService):
    def __init__(self, url, database, **opts):
        CouchService.__init__(self, url = url,
                              database = database,
                              **opts)


    @CouchUtils.connectToCouch
    def getDataCollection(self, collName, user = "cmsdataops",
                          group = "cmsdataops"):
        """
        _getDataCollection_

        Get a data collection by name
        """
        coll = CouchCollection(name = collName, database = self.database,
                               url = self.url)

        coll.owner = self.newOwner(group, user)
        coll.populate()
        return coll

    @CouchUtils.connectToCouch
    def failedJobs(self, failedJobs):
        """
        _failedJobs_

        Given a list of failed jobs, sort them into Filesets and record them

        NOTE: jobs must have a non-standard task, workflow, owner and group
        attributes assigned to them.
        """
        for job in failedJobs:
            try:
                taskName = job['task']
                workflow = job['workflow']
            except KeyError, ex:
                msg =  "Missing required, non-standard key %s in job in ACDC.DataCollectionService" % (str(ex))
                logging.error(msg)
                raise ACDCDCSException(msg)

            coll = CouchCollection(database = self.database, url = self.url,
                                   name = workflow,
                                   type = CollectionTypes.DataCollection)
            owner = self.newOwner(job.get("group", "cmsdataops"),
                                  job.get("owner", "cmsdataops"))
            coll.setOwner(owner)
            fileset = CouchFileset(database = self.database, url = self.url,
                                    name = taskName)
            coll.addFileset(fileset)
            fileset.add(files = job['input_files'], mask = job['mask'])

        return

    @CouchUtils.connectToCouch
    def chunkFileset(self, collectionName, filesetName, chunkSize = 100,
                     user = "cmsdataops", group = "cmsdataops"):
        """
        _chunkFileset_

        Split all of the fileset in a given collection/task into chunks.  This
        will return a list of dictionaries that contain the offset into the
        fileset and a summary of files/events/lumis that are in the fileset
        chunk.
        """
        chunks = []
        results = self.couchdb.loadView("ACDC", "owner_coll_fileset_metadata",
                                        {"startkey": [group, user,
                                                      collectionName, filesetName],
                                         "endkey": [group, user,
                                                    collectionName, filesetName, {}]}, [])

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for row in results["rows"]:
            if currentLocation == None:
                currentLocation = row["key"][4]
            if numFilesInBlock == chunkSize or currentLocation != row["key"][4]:
                chunks.append({"offset": totalFiles, "files": numFilesInBlock,
                               "events": numEventsInBlock, "lumis": numLumisInBlock,
                               "locations": currentLocation})
                totalFiles += numFilesInBlock
                currentLocation = row["key"][4]
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
    def singleChunkFileset(self, collectionName, filesetName,
                           user = "cmsdataops", group = "cmsdataops"):
        """
        _singleChunkFileset_

        Put all of the fileset in a given collection/task into a single chunk.  This
        will return a dictionary that contains the offset into the
        fileset and a summary of files/events/lumis that are in the fileset
        chunk.
        """
        results = self.couchdb.loadView("ACDC", "owner_coll_fileset_metadata",
                                        {"startkey": [group, user,
                                                      collectionName, filesetName],
                                         "endkey": [group, user,
                                                    collectionName, filesetName, {}]}, [])

        locations = set()
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for row in results["rows"]:
            locationsInFile = row["key"][4]
            locations |= set(locationsInFile)

            numFilesInBlock += 1
            numLumisInBlock += row["value"]["lumis"]
            numEventsInBlock += row["value"]["events"]

        return {"offset": 0, "files": numFilesInBlock,
                "events": numEventsInBlock, "lumis": numLumisInBlock,
                "locations": locations}

    @CouchUtils.connectToCouch
    def getChunkInfo(self, collectionName, filesetName, chunkOffset, chunkSize,
                     user = "cmsdataops", group = "cmsdataops"):
        """
        _getChunkInfo_

        Retrieve metadata for a particular chunk.
        """
        results = self.couchdb.loadView("ACDC", "owner_coll_fileset_metadata",
                                        {"startkey": [group, user,
                                                      collectionName, filesetName],
                                         "endkey": [group, user,
                                                    collectionName, filesetName, {}],
                                         "skip": chunkOffset,
                                         "limit": chunkSize}, [])

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for row in results["rows"]:
            if currentLocation == None:
                currentLocation = row["key"][4]

            numFilesInBlock += 1
            numLumisInBlock += row["value"]["lumis"]
            numEventsInBlock += row["value"]["events"]

        return {"offset": totalFiles, "files": numFilesInBlock,
                "events": numEventsInBlock, "lumis": numLumisInBlock,
                "locations": currentLocation}

    @CouchUtils.connectToCouch
    def getChunkFiles(self, collectionName, filesetName, chunkOffset, chunkSize = 100,
                      user = "cmsdataops", group = "cmsdataops"):
        """
        _getChunkFiles_

        Retrieve a chunk of files from the given collection and task.
        """
        chunkFiles = []
        result = self.couchdb.loadView("ACDC", "owner_coll_fileset_files",
                                       {"startkey": [group, user,
                                                     collectionName, filesetName],
                                        "endkey": [group, user,
                                                   collectionName, filesetName, {}],
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
    def getLumiWhitelist(self, collectionID, taskName, user = "cmsdataops",
                         group = "cmsdataops"):
        """
        _getLumiWhitelist_

        Query ACDC for all of the files in the given collection and task.
        Generate a run and lumi whitelist for the given files with the following
        format:
          {"run1": [[lumi1, lumi4], [lumi6, lumi10]],
           "run3": [lumi5, lumi10]}

        Note that the run numbers are strings.
        """
        results = self.couchdb.loadView("ACDC", "owner_coll_fileset_files",
                                        {"startkey": [group, user,
                                                      collectionID, taskName],
                                         "endkey": [group, user,
                                                    collectionID, taskName, {}]}, [])

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
