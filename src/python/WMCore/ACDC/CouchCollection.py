#!/usr/bin/env python
# encoding: utf-8
"""
CouchCollection.py

Created by Dave Evans on 2010-03-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest


from WMCore.ACDC.Collection import Collection
import WMCore.Database.CMSCouch as CMSCouch
from WMCore.ACDC.CouchUtils import connectToCouch, requireOwner



class CouchCollection(Collection):
    def __init__(self, **options):
        Collection.__init__(self, **options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None

    @connectToCouch
    @requireOwner
    def create(self):
        """
        _create_

        create new Collection in the couch backend using the arguments in this object

        """
        self.getCollectionId()
        if self['collection_id'] != None:
            print "collection exists..."
            return self.get()
        document    = CMSCouch.Document()
        document['collection'] = self
        del document['collection']['database']
        del document['collection']['url']
        commitInfo = self.couchdb.commitOne( document )
        commitInfo = commitInfo[0]
        self['collection_id'] = str(commitInfo['id'])
        document['_id'] = commitInfo['id']
        document['_rev'] = commitInfo['rev']
        self.owner.ownThis(document)
        return
        
    @connectToCouch
    @requireOwner
    def getCollectionId(self):
        """
        _getCollectionId_
        
        Use owner and collection name to retrieve the ID of the collection
        """
        result = self.couchdb.loadView("ACDC", 'collection_name',
             {'startkey' :[self['name'], self.owner.group['name'], self.owner['name']],
               'endkey' : [self['name'], self.owner.group['name'], self.owner['name']]}, []
            )
        
        if len(result['rows']) == 0:
            doc = None
        else:
            doc = result['rows'][0]['value']
            self['collection_id'] = str(doc['_id'])
    
    @connectToCouch
    @requireOwner
    def get(self):
        """
        _get_
        
        
        """
        doc = None
        if self['collection_id'] == None:
            self.getCollectionId()
        try:
            doc = self.couchdb.document(self['collection_id'])
        except CMSCouch.CouchNotFoundError as ex:
            doc = None
        if doc == None:
            return None
        self.unpackDoc(doc)
        return self
        
    @connectToCouch
    @requireOwner
    def drop(self):
        """
        _drop_
        
        Drop this collection
        
        TODO: Fail if it has filesets existing.
        
        """
        self.getCollectionId()
        if self['collection_id'] == None:
            #document doesnt exist, cant delete
            return 
        self.couchdb.delete_doc(self['collection_id'])
        
        
    def unpackDoc(self, doc):
        """
        _unpackDoc_

        Util to unpack  
        """
        leaveAlone = ['database', 'url', 'filesets']
        [self.__setitem__(str(k), str(v)) for k,v in doc[u'collection'].items() if k not in leaveAlone ]
        self['collection_id'] = str(doc[u'_id'])


from WMCore.GroupUser.User import makeUser

class CouchCollectionTests(unittest.TestCase):
    def setUp(self):
        self.url = "127.0.0.1:5984"
        self.database = "acdc2"
        self.owner = makeUser("DMWM", "evansde77", self.url, self.database)
        self.owner.connect()
        self.owner.create()
        
    def tearDown(self):
        
        self.owner.drop()
        self.owner.group.drop()
        pass

    def testA(self):
        """
        create the collection
        
        """
        
        collection = CouchCollection(database = self.database, url = self.url, name = "Thunderstruck")
        collection.setOwner(self.owner)
        collection.create()
        
        collection.getCollectionId()
        
        print self.owner.couch.document(collection['collection_id'])
        
        collection.drop()
       
        
if __name__ == '__main__':
    unittest.main()