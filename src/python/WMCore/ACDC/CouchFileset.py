#!/usr/bin/env python
# encoding: utf-8
"""
CouchFileset.py

Created by Dave Evans on 2010-03-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""



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
    _CouchFileset_
    
    Create a fileset that can be stored/read from Couch
    
    """
    def __init__(self, **options):
        """
        Required Options:
        
        url - couch db instance URL
        database - couch db instance name
        dataset - name/label for fileset
        
        Will also need to call setCollection with a valid collection before being able to create
        a fileset in couch
        """
        Fileset.__init__(self, **options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None



    @connectToCouch
    @requireOwner
    @requireCollection
    def create(self):
        """
        create this fileset
        """
        check = self.get()
        if check != None:
            return check
        document    = CMSCouch.Document()
        document['fileset'] = self
        del document['fileset']['database']
        del document['fileset']['url']
        commitInfo = self.couchdb.commitOne( document )
        self['fileset_id'] = commitInfo[0]['id']
        document['_id'] = commitInfo[0]['id']
        document['_rev'] = commitInfo[0]['rev']
        self.owner.ownThis(document)
        return
        
    @connectToCouch
    @requireOwner
    @requireCollection
    def getFilesetId(self):
        """getFilesetId
        
        map owner/collection/dataset triplet to the document ID
        """
        result = self.couchdb.loadView("ACDC", 'fileset_owner_coll_dataset',
             {'startkey' :[self.owner.group.name, self.owner.name, self['collection_id'], self['dataset']],
               'endkey' : [self.owner.group.name, self.owner.name, self['collection_id'], self['dataset']]}, []
            )
        if len(result['rows']) == 0:
            doc = None
        else:
            doc = result['rows'][0]['value']
        return
        
    @connectToCouch
    @requireOwner
    @requireCollection
    def get(self):
        """
        _get_
        
        retrieve the fileset from Couch
        """
        doc = None
        if self['fileset_id'] == None:
            self.getFilesetId()
            
        result = self.couchdb.loadView("ACDC", 'fileset_id',
                     {'startkey' :[self['fileset_id']],
                       'endkey' : [self['fileset_id']]}, []
                    )
        if len(result['rows']) == 0:
            doc = None
            return None
        else:
            doc = result['rows'][0]['value']
            self.unpackDoc(doc)
            return self
        
    def unpackDoc(self, doc):
        """
        _unpackDoc_

        Util to unpack 
        """
        leaveAlone = ['fileset_id', 'database', 'url']
        [self.__setitem__(str(k), str(v)) for k,v in doc[u'fileset'].items() if k not in leaveAlone ]
        self['fileset_id'] = str(doc[u'_id'])
        
    @connectToCouch
    @requireOwner
    @requireCollection
    @requireFilesetId   
    def drop(self):
        """
        _drop_
        
        Remove this fileset
        """
        self.getFilesetId()
        if self['fileset_id'] == None:
            #document doesnt exist, cant delete
            return 
        self.couchdb.delete_doc(self['fileset_id'])
        
    @connectToCouch
    @requireOwner
    @requireCollection
    @requireFilesetId  
    def add(self, *files):
        """
        _add_
        
        Add Files to this fileset
        files should be a list of WMCore.WMBS.File objects
        """
        try:
            doc = self.couchdb.document(self['fileset_id'])
        except CMSCouch.CouchNotFoundError as ex:
            msg = "Document for fileset with ID: %s\n" % self['fileset_id']
            msg += "not found, cannot add files to fileset"
            raise RuntimeError, msg
            
        
        jsonFiles = {}
        [ jsonFiles.__setitem__(f['lfn'], f.__to_json__(None)) for f in files]
        doc[u'fileset'][u'files'].update( jsonFiles)
        
        self.couchdb.commitOne(doc)
    
    @connectToCouch
    @requireOwner
    @requireCollection
    @requireFilesetId 
    def files(self):
        """
        _files_
        
        return an iterator over the files contained in this document
        
        TODO: Look at paging this with a custom iterator object if possible, 
        for now, just pull the doc and yield the files
        
        """
        try:
            doc = self.couchdb.document(self['fileset_id'])
        except CMSCouch.CouchNotFoundError as ex:
            msg = "Unable to retrieve Couch Document for fileset"
            msg += str(msg)
            raise RuntimeError, msg
        
        files = doc[u'fileset'][u'files']
        for d in files.values():
            yield d
        
    @connectToCouch
    @requireOwner
    @requireCollection
    @requireFilesetId  
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
    @requireOwner
    @requireCollection
    @requireFilesetId   
    def filecount(self):
        """
        _filecount_
        
        Check how many files are in this fileset
        
        """
        result = self.couchdb.loadView("ACDC", 'fileset_file_count',
             {'startkey' :[self['fileset_id']],
               'endkey' : [self['fileset_id']]}, []
            )
        rows = result[u'rows']
        if len(rows) == 0:
            msg = "Cant find filecount for document id: %s" %self['fileset_id']
            raise RuntimeError, msg
            
        count = rows[0][u'value']
        return count

