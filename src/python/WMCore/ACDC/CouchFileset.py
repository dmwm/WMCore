#!/usr/bin/env python
# encoding: utf-8
"""
CouchFileset.py

Created by Dave Evans on 2010-03-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
import time

import WMCore.Database.CMSCouch as CMSCouch
from WMCore.ACDC.Fileset import Fileset
from WMCore.Algorithms.ParseXMLFile import coroutine
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset as DataStructsFileset
from WMCore.DataStructs.Run import Run
from WMCore.Database.CouchUtils import connectToCouch, requireFilesetName


@coroutine
def makeRun(targets):
    """
    _makeRun_

    create a DataStruct Run instance form the Couch JSON dict
    """
    while True:
        fileRef, runs = (yield)
        for run in runs:
            newRun = Run(run[u'Run'], *run[u'Lumis'])
            fileRef.addRun(newRun)


@coroutine
def filePipeline(targets):
    """
    _conversionPipeline_

    """
    while True:
        inputDict = (yield)
        newFile = File(
            lfn=str(inputDict[u'lfn']),
            size=int(inputDict[u'size']),
            events=int(inputDict[u'events'])
        )
        targets['run'].send((newFile, inputDict[u'runs']))
        targets['fileset'].addFile(newFile)


class CouchFileset(Fileset):
    def __init__(self, **options):
        Fileset.__init__(self, **options)
        self.url = options.get('url')
        self.database = options.get('database')
        self['name'] = options.get('name')
        self.server = None
        self.couchdb = None

    @connectToCouch
    def drop(self):
        """
        _drop_

        Remove this fileset

        This is racy. someone can add to the fileset before we get done
        deleting it. Actually, they can delete after we get done deleting
        it too. Oh well.
        """
        for d in self.filelistDocuments():
            self.couchdb.delete_doc(d)

    @connectToCouch
    @requireFilesetName
    def filelistDocuments(self):
        """
        _filelistDocuments_

        Get a list of document ids corresponding to filelists in this fileset
        """
        params = {"startkey": [self.collectionName, self["name"]],
                  "endkey": [self.collectionName, self["name"]],
                  "reduce": False}
        result = self.couchdb.loadView("ACDC", "coll_fileset_docs",
                                       params)

        docs = [row["id"] for row in result["rows"]]
        return docs

    @connectToCouch
    @requireFilesetName
    def add(self, files, mask=None):
        """
        _add_

        Add files to this fileset

        Note: if job was lumi based splitted, then we do not have
        reliable events information. If job was event based splitted,
        then we do not have reliable lumi information.
        """
        filteredFiles = []
        if mask:
            for f in files:
                # There might be no LastEvent for last job of a file
                if mask['LastEvent'] and mask['FirstEvent']:
                    f['events'] = mask['LastEvent'] - mask['FirstEvent'] + 1
                    f['first_event'] = mask['FirstEvent']
                elif mask['FirstEvent']:
                    f['events'] = f['events'] - mask['FirstEvent'] + 1
                    f['first_event'] = mask['FirstEvent']

            maskLumis = mask.getRunAndLumis()
            if maskLumis != {}:
                # Then we actually have to do something
                for f in files:
                    newRuns = mask.filterRunLumisByMask(runs=f['runs'])
                    if newRuns != set([]):
                        f['runs'] = newRuns
                        filteredFiles.append(f)
            else:
                # Likely real data with EventBased splitting
                filteredFiles = files
        else:
            filteredFiles = files

        jsonFiles = {}
        for f in filteredFiles:
            jsonFiles.__setitem__(f['lfn'], f.__to_json__(None))
        fileList = self.makeFilelist(jsonFiles)
        return fileList

    @connectToCouch
    @requireFilesetName
    def makeFilelist(self, files=None):
        """
        _makeFilelist_

        Create a new filelist document containing the id
        """
        files = files or {}
        # add a version to each of these ACDC docs such that we can properly
        # parse them and avoid issues between ACDC docs and agent base code
        input = {"collection_name": self.collectionName,
                 "collection_type": self.collectionType,
                 "fileset_name": self["name"],
                 "files": files,
                 "acdc_version": 2,
                 "timestamp": time.time()}

        document = CMSCouch.Document(None, input)

        commitInfo = self.couchdb.commitOne(document)
        document['_id'] = commitInfo[0]['id']
        if 'rev' in commitInfo[0]:
            document['_rev'] = commitInfo[0]['rev']
        else:
            if commitInfo[0]['reason'].find('{exit_status,0}') != -1:
                # TODO: in this case actually insert succeeded but return error
                # due to the bug
                # https://issues.apache.org/jira/browse/COUCHDB-893
                # if rev is needed to proceed need to get by
                # self.couchdb.documentExist(document['_id'])
                # but that function need to be changed to return _rev
                document['_rev'] = "NeedToGet"
            else:
                msg = "Unable to insert document: check acdc server doc id: %s" % document['_id']
                raise RuntimeError(msg)
        return document

    @connectToCouch
    def listFiles(self):
        """
        _listFiles_

        return an iterator over the files contained in this fileset
        """
        for filelist in self.filelistDocuments():
            try:
                doc = self.couchdb.document(filelist)
            except CMSCouch.CouchNotFoundError as ex:
                msg = "Unable to retrieve Couch Document for fileset"
                msg += str(msg)
                raise RuntimeError(msg)

            files = doc["files"]
            for d in files.values():
                yield d

    @connectToCouch
    def fileset(self):
        """
        _fileset_

        Make a WMCore.DataStruct.Fileset instance containing the files in this fileset

        """
        result = DataStructsFileset(self['name'])
        pipeline = filePipeline({'fileset': result, 'run': makeRun({})})
        for f in self.listFiles():
            pipeline.send(f)
        return result

    @connectToCouch
    @requireFilesetName
    def populate(self):
        """
        _populate_

        Load all files out of couch.
        """
        params = {"startkey": [self.collectionName, self["name"]],
                  "endkey": [self.collectionName, self["name"]],
                  "include_docs": True, "reduce": False}
        result = self.couchdb.loadView("ACDC", "coll_fileset_docs",
                                       params)
        self.files = {}
        for row in result["rows"]:
            self.files.update(row["doc"]["files"])
            self["files"] = self.files
        return

    def fileCount(self):
        """
        _fileCount_

        Determine how many files are in the fileset.
        """
        params = {"startkey": [self.collectionName, self["name"]],
                  "endkey": [self.collectionName, self["name"]],
                  "reduce": True, "group_level": 2}
        result = self.couchdb.loadView("ACDC", "coll_fileset_count",
                                       params)
        return result["rows"][0]["value"]
