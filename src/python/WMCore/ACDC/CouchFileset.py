#!/usr/bin/env python
# encoding: utf-8
"""
CouchFileset.py

Created by Dave Evans on 2010-03-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""


from WMCore.ACDC.Fileset import Fileset
from WMCore.Database.CouchUtils import connectToCouch, requireOwner, requireCollection, requireFilesetName

import WMCore.Database.CMSCouch as CMSCouch

from WMCore.Algorithms.ParseXMLFile import coroutine

from WMCore.DataStructs.Fileset import Fileset as DataStructsFileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run

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
            lfn = str(inputDict[u'lfn']),
            size = int(inputDict[u'size']),
            events = int(inputDict[u'events'])
        )
        targets['run'].send(  (newFile, inputDict[u'runs'])  )
        targets['fileset'].addFile(newFile)



class CouchFileset(Fileset):
    def __init__(self, **options):
        Fileset.__init__(self, **options)
        self.url          = options.get('url')
        self.database     = options.get('database')
        self['name']      = options.get('name')
        self.server       = None
        self.couchdb      = None

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
        params = {"startkey": [self.owner.group.name, self.owner.name,
                               self.collectionName, self["name"]],
                  "endkey": [self.owner.group.name, self.owner.name,
                             self.collectionName, self["name"]],
                  "reduce": False}
        result = self.couchdb.loadView("ACDC", "owner_coll_fileset_docs",
                                       params)

        docs = [row["id"] for row in result["rows"]]
        return docs

    @connectToCouch
    @requireFilesetName
    def add(self, files, mask = None):
        """
        _add_

        Add files to this fileset
        """
        filteredFiles = []
        if mask:
            maskLumis = mask.getRunAndLumis()
            if maskLumis != {}:
                # Then we actually have to do something
                for f in files:
                    newRuns = mask.filterRunLumisByMask(runs = f['runs'])
                    if newRuns != set([]):
                        f['runs'] = newRuns
                        filteredFiles.append(f)
            else:
                filteredFiles = files
        else:
            filteredFiles = files

        jsonFiles = {}
        [ jsonFiles.__setitem__(f['lfn'], f.__to_json__(None)) for f in filteredFiles]
        filelist = self.makeFilelist(jsonFiles)
        return filelist

    @connectToCouch
    @requireOwner
    @requireFilesetName
    def makeFilelist(self, files = {}):
        """
        _makeFilelist_

        Create a new filelist document containing the id
        """
        input = {"collection_name": self.collectionName,
                 "collection_type": self.collectionType,
                 "fileset_name": self["name"],
                 "files": files}

        document = CMSCouch.Document(None, input)
        self.owner.ownThis(document)

        commitInfo = self.couchdb.commitOne(document)
        document['_id'] = commitInfo[0]['id']
        document['_rev'] = commitInfo[0]['rev']
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
                raise RuntimeError, msg

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
        pipeline = filePipeline({'fileset' : result, 'run' : makeRun({}) })
        for f in self.listFiles():
            pipeline.send(f)
        return result

    @connectToCouch
    @requireOwner
    @requireFilesetName
    def populate(self):
        """
        _populate_

        Load all files out of couch.
        """
        params = {"startkey": [self.owner.group.name, self.owner.name,
                               self.collectionName, self["name"]],
                  "endkey": [self.owner.group.name, self.owner.name,
                             self.collectionName, self["name"]],
                  "include_docs": True, "reduce": False}
        result = self.couchdb.loadView("ACDC", "owner_coll_fileset_docs",
                                       params)

        self.files = {}
        for row in result["rows"]:
            self.files.update(row["doc"]["files"])
        return

    def fileCount(self):
        """
        _fileCount_

        Determine how many files are in the fileset.
        """
        params = {"startkey": [self.owner.group.name, self.owner.name,
                               self.collectionName, self["name"]],
                  "endkey": [self.owner.group.name, self.owner.name,
                             self.collectionName, self["name"]],
                  "reduce": True, "group_level": 4}
        result = self.couchdb.loadView("ACDC", "owner_coll_fileset_count",
                                       params)
        return result["rows"][0]["value"]
