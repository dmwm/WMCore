#!/usr/bin/env python
# encoding: utf-8
"""
CouchFileset.py

Created by Dave Evans on 2010-03-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import random

from WMCore.ACDC.Fileset import Fileset
import WMCore.Database.CMSCouch as CMSCouch
from WMCore.ACDC.CouchUtils import connectToCouch, requireOwner, requireCollection, requireFilesetId
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
    """
    CouchFileset with 100% more awesomes than the first one
    """
    def __init__(self, **options):
        Fileset.__init__(self, **options)
        self.url      = options.get('url', None)
        self.database = options.get('database', None)
        self.server   = None
        self.couchdb  = None


    @connectToCouch
    def get(self):
        """
        _get_
    
        """
        docid = dict.get(self, '_id', None)
        filesetId = dict.get(self, 'fileset_id', None) 
        if docid == None:
            docid = filesetId
        if docid == None:
            msg = "No document ID set to load Fileset from Couch"
            raise RuntimeError, msg

        self.update(self.couchdb.document(docid))
        return        
        
    @connectToCouch
    @requireOwner
    @requireCollection
    def create(self):
        """
        create this fileset
        """
        if self.exists():
            self.get()
            return
        filesetInfo = dict(self)
        del filesetInfo['url']
        del filesetInfo['database']
        document    = CMSCouch.Document(None, {"fileset" : filesetInfo})
        commitInfo = self.couchdb.commitOne( document )
        self['fileset_id'] = commitInfo[0]['id']
        self['_id'] = commitInfo[0]['id']
        self['_rev'] = commitInfo[0]['rev']
        document['_id'] = self['_id']
        document['_rev'] = self['_rev']
        self.owner.ownThis(document)
        return
    
    
    @connectToCouch
    @requireOwner
    @requireCollection
    def exists(self):
        """

        map owner/collection/dataset triplet to the document ID
        """
        result = self.couchdb.loadView("ACDC", 'fileset_owner_coll_dataset',
             {'startkey' :[self.owner.group.name, self.owner.name, self['collection_id'], self['dataset']],
               'endkey' : [self.owner.group.name, self.owner.name, self['collection_id'], self['dataset']]}, []
            )
        if len(result['rows']) == 0:
            return False
        else:
            self['_id'] = result['rows'][0]['id']
            return True


    @connectToCouch
    def drop(self):
        """
        _drop_

        Remove this fileset
        """
        if dict.get(self, '_id', None) != None: 
            for d in self.filelistDocuments():
                self.couchdb.delete_doc(d)
            self.couchdb.delete_doc(self['_id'])


    @connectToCouch
    def makeFilelist(self, files = {}):
        """
        _makeFilelist_
        
        Create a new filelist document containing the id of the fileset
        """
        document = CMSCouch.Document(None, {"filelist" : { "fileset_id" : self['_id'], "files": files,
                                                           "collection_id": self["collection_id"],
                                                           "task": dict.get(self, "task", None)} })
        commitInfo = self.couchdb.commitOne( document )
        document['_id'] = commitInfo[0]['id']
        document['_rev'] = commitInfo[0]['rev']
        return document
        
    @connectToCouch
    def filelistDocuments(self):
        """
        _filelistDocuments_
        
        Get a list of document ids corresponding to filelists in this fileset
        """
        result = self.couchdb.loadView("ACDC", 'fileset_filelist_ids', {'startkey' : [self['_id']], "endkey": [self['_id']]} )
        docs = [ row[u'value'] for row in result[u'rows'] ]
        return docs
        
    @connectToCouch  
    def add(self, files, mask):
        """
        _add_
        
        Add files to this fileset
        """
        maskLumis = mask.getRunAndLumis()
        if maskLumis != {}:
            for f in files:
                for r in f['runs']:
                    newRun = Run()
                    if not r.run in maskLumis.keys():
                        # Then it's not in there
                        continue
                    newRun.run = r.run
                    for lumi in r.lumis:
                        if lumi in maskLumis[r.run]:
                            # Then we add it
                            newRun.lumis.append(lumi)
                    f['runs'].remove(r)
                    if len(newRun.lumis) > 0:
                        # Add it
                        f['runs'].append(newRun)
                            
                            
        jsonFiles = {}
        [ jsonFiles.__setitem__(f['lfn'], f.__to_json__(None)) for f in files]
        filelist = self.makeFilelist(jsonFiles)
        return filelist

    @connectToCouch          
    def files(self):
        """
        _files_

        return an iterator over the files contained in this document


        """
        for filelist in self.filelistDocuments():
            try:
                doc = self.couchdb.document(filelist)
            except CMSCouch.CouchNotFoundError as ex:
                msg = "Unable to retrieve Couch Document for fileset"
                msg += str(msg)
                raise RuntimeError, msg
            
            files = doc[u'filelist'][u'files']
            for d in files.values():
                yield d

    @connectToCouch      
    def fileset(self):
        """
        _fileset_
    
        Make a WMCore.DataStruct.Fileset instance containing the files in this fileset
    
        """
        result = DataStructsFileset(self['dataset'])
        pipeline = filePipeline({'fileset' : result, 'run' : makeRun({}) })
        for f in self.files():
            pipeline.send(f)
        return result
         
    @connectToCouch               
    def filecount(self):
        """
        count files in fileset
        
        Todo: Update map/reduce for this & implement
        """
        result = self.couchdb.loadView("ACDC", 'fileset_file_count',
                                       {'startkey' : [self['_id']], "endkey": [self['_id']]})
 
        return result["rows"][0]["value"]
         

