#!/usr/bin/env python
# encoding: utf-8
"""
CouchService.py

Created by Dave Evans on 2010-04-20.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
from WMCore.ACDC.Service import Service
from WMCore.ACDC.CouchOwner import CouchOwner
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
import WMCore.ACDC.CouchUtils as CouchUtils
import WMCore.Database.CMSCouch as CMSCouch



class CouchService(Service):
    
    def __init__(self, **options):
        Service.__init__(self, **options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None

    @CouchUtils.connectToCouch
    def listOwners(self):
        """
        _listOwners_
        
        List the owners in the DB
        
        """
        result = self.couchdb.loadView("ACDC", 'owner_name_group', {}, [])
        for row in result[u'rows']:
            print row
        
    @CouchUtils.connectToCouch
    def listCollections(self, owner):
        """
        _listCollections_
        
        List the collections belonging to an owner
        
        """
        result = self.couchdb.loadView("ACDC", 'owner_listcollections',
             {'startkey' :[owner['owner_id']],
               'endkey' : [owner['owner_id']]}, []
            )
    

        for row in result[u'rows']:
            coll = CouchCollection(collection_id = row[u'value'][u'collection_id'], 
                                   database = self.database, url = self.url)
            coll.setOwner(owner)
            coll.get()
            yield coll
            
    @CouchUtils.connectToCouch
    def listFilesets(self, collection):
        """
        _listFilesets_
        
        List the Filesets belonging to the collection provided
        
        """
        result = self.couchdb.loadView("ACDC", 'collection_listfilesets',
             {'startkey' :[collection['collection_id']],
               'endkey' : [collection['collection_id']]}, []
            )
        for row in result[u'rows']:
            fset = CouchFileset(fileset_id = row[u'value'][u'_id'], database = self.database, url = self.url)
            fset.setCollection(collection)
            fset.get()
            yield fset
    
    @CouchUtils.connectToCouch
    def removeOwner(self, owner):
        """
        _removeOwner_
        
        Remove an owner and all the associated collections and filesets
        
        """
        result = self.couchdb.loadView("ACDC", 'owner_colls_and_filesets',
             {'startkey' :[owner['owner_id']],
               'endkey' : [owner['owner_id']]}, []
            )
        for row in result[u'rows']:
            deleteMe = CMSCouch.Document()
            deleteMe[u'_id'] = row[u'value'][u'_id']
            deleteMe[u'_rev'] = row[u'value'][u'_rev']
            deleteMe.delete()
            self.couchdb.queue(deleteMe)
        self.couchdb.commit()
        owner.drop()
        return
        
    @CouchUtils.connectToCouch
    def dumpEverything(self):
        result = self.couchdb.loadView("ACDC", 'owner_colls_and_filesets',
             {}, []
            )
        for row in result[r'rows']:
            deleteMe = CMSCouch.Document()
            deleteMe[u'_id'] = row[u'value'][u'_id']
            deleteMe[u'_rev'] = row[u'value'][u'_rev']
            deleteMe.delete()
            self.couchdb.queue(deleteMe)
        self.couchdb.commit()