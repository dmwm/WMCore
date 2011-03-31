#!/usr/bin/env python
# encoding: utf-8
"""
WorkloadSummary.py

Created by Dave Evans on 2011-01-07.
Copyright (c) 2011 Fermilab. All rights reserved.
"""
from urllib2 import quote
import WMCore.Database.CMSCouch as CMSCouch
from WMCore.Database.CMSCouch import Document
from WMCore.ACDC.CouchUtils import connectToCouch


def prototypePerformanceDoc(workload, task):
    return  {
        "performance" : {
        "workload" : workload,
        "task" : task,  
        "data" : {},
        }
    }

class WorkloadSummary(Document):
    """
    _WorkloadSummary_
    
    """
    def __init__(self, workloadId, url, database, workload = None):
        Document.__init__(self, workloadId)
        self.database = database
        self.url = url
        self.server = None
        self.couchdb = None
        if workload != None:
            self.update(workload.generateWorkloadSummary())
        
    @connectToCouch
    def create(self):
        """
        _create_
        
        First time save that builds the task performance documents

        """
        self.buildPerformanceDocs()
        self.save()
        
        
    @connectToCouch
    def save(self):
        """
        _save_
        
        persist workload summary to couch
        """
        rawResults = self.couchdb.commit(doc = self)

        # We should only be committing one document at a time
        # if not, get the last one.

        try:
            commitResults = rawResults[-1]
            self.rev = commitResults.get('rev')
            self.id  = commitResults.get('id')
        except KeyError, ex:
            msg  = "Document returned from couch without ID or Revision\n"
            msg += "Document probably bad\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError(message = msg)
        
        
        
    def buildPerformanceDocs(self):
        """
        _buildPerformanceDocs_
        
        Create a set of documents, one for each task that will contain the long term
        performance records for the task
        """
        added = []
        for t, docid in self['performance'].items():
            if docid != None:
                document = Document(None, prototypePerformanceDoc(self['_id'], t))
                self.couchdb.queue(document)
                added.append(t)
        results = self.couchdb.commit()
        for t, r in zip(added, results):
            self['performance'][t] = str(r[u'id'])
        
            
        
    @connectToCouch
    def load(self):
        """
        _load_
        
        load document content from couch, assumes that the workload id has been provided
        """
        if self['_id'] == None:
            msg = "No workload ID provided, cant load WorkloadSummary without it"
            raise RuntimeError, msg
        try:
            doc = self.couchdb.document(self['_id'])
            self.update(doc)
        except CMSCouch.CouchError, err:
            msg = "Error loading document for workload summary: %s\n" % self['_id']
            msg += str(err)
            raise RuntimeError(msg)

        
    @connectToCouch 
    def addACDCFileset(self, task, fileset):
        """
        _addACDCFileset_
        
        Add the fileset ID for an ACDC Fileset associated to a given task
        """
        update = "updateacdc"
        updateUri = "/" + self.couchdb.name + "/_design/WorkloadSummary/_update/"+ update + "/" + self['_id']
        argsstr = "?task=%s&fileset=%s" % (quote(task), quote(fileset))
        updateUri += argsstr
        self.couchdb.makeRequest(uri = updateUri, type = "PUT", decode = False)
        return
        
    @connectToCouch         
    def addACDCCollection(self, collname):
        """
        _addACDCCollection_
        """
        update = "updatecollection"
        updateUri = "/" + self.couchdb.name + "/_design/WorkloadSummary/_update/"+ update + "/" + self['_id']
        argsstr = "?collection=%s" % quote(collname)
        updateUri += argsstr
        self.couchdb.makeRequest(uri = updateUri, type = "PUT", decode = False)
        
        