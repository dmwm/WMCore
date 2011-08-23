#!/usr/bin/env python
# encoding: utf-8
"""
AnalCollectionInterface.py

Made by Andrew Melo <andrew.m.melo@vanderbilt.edu>
    and Eric Vaandering <ewv@fnal.gov>
Stolen from Dave Evans <evansde@fnal.gov>
"""


import WMCore.ACDC.CollectionTypes as CollectionTypes
import WMCore.ACDC.CouchUtils as CouchUtils

from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchService import CouchService
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run

from DBSAPI.dbsApi import DbsApi



class AnalysisCollectionService(CouchService):
    def __init__(self, url, database, **opts):
        CouchService.__init__(self, url = url,
                              database = database,
                              **opts)


    @CouchUtils.connectToCouch
    def createCollection(self, collectionName, userName, groupName):
        """
        _createCollection_

        Create an empty AnalysisCollection
        """

        if userName == None:
            msg = "WMSpec does not contain an owner.name parameter"
            raise RuntimeError(msg)
        if groupName == None:
            msg = "WMSpec does not contain an owner.group parameter"
            raise RuntimeError(msg)


        user = self.newOwner(groupName, userName)
        collection = CouchCollection(
            name = collectionName, type = CollectionTypes.AnalysisCollection,
            url = self.url, database = self.database
            )
        collection.setOwner(user)
        collection.create()

        return collection


    @CouchUtils.connectToCouch
    def createFilesetFromDBS(self, collection, filesetName, dbsURL, dataset, mask=None):
        """
        _createFilesetFromDBS_

        Get info from DBS, apply mask (filter) and create a fileset
        """
        fileSet = CouchFileset(database = self.database, url = self.url,
                               name = filesetName)
        fileSet.setCollection(collection)

        files = []

        args = {}
        args["url"] = dbsURL
        args["version"] = "DBS_2_0_9"
        args["mode"] = "GET"
        dbsApi = DbsApi(args)
        dbsResults = dbsApi.listFiles(path=dataset, retriveList=["retrive_lumi", "retrive_run"])

        for dbsResult in dbsResults:
            file = File(lfn=dbsResult["LogicalFileName"], size=dbsResult["FileSize"],
                        events=dbsResult["NumberOfEvents"])
            runs = {}
            for lumi in dbsResult["LumiList"]:
                runNumber = lumi['RunNumber']
                runString = str(runNumber)
                lumiNumber = lumi["LumiSectionNumber"]
                if runString in runs:
                    runs[runString].lumis.append(lumiNumber)
                else:
                    runs[runString] = Run(runNumber, lumiNumber)

            for run in runs.values():
                file.addRun(run)
            files.append(file)

        fileList = fileSet.add(files, mask)
        return fileSet, fileList
