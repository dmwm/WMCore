#!/usr/bin/env python
# encoding: utf-8
"""
CouchOwner.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.ACDC.Owner import Owner
import WMCore.Database.CMSCouch as CMSCouch
from WMCore.ACDC.CouchUtils import connectToCouch, requireOwnerId



class CouchOwner(Owner):
    def __init__(self, **options):
        Owner.__init__(self, **options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None

    @connectToCouch
    def create(self):
        """
        _create_
        
        create new Owner in the couch backend using the arguments in this object
        
        """
        self.getOwnerId()
        if self['owner_id'] != None:
            return self.get() 
        document    = CMSCouch.Document()
        document['owner'] = self
        #TODO: prune database and url keys...
        commitInfo = self.couchdb.commitOne( document )
        commitInfo = commitInfo[0]
        self['owner_id'] = str(commitInfo[u'id'])
        return
        
    @connectToCouch
    def getOwnerId(self):
        """
        _getOwnerId_
        
        Look up the owner id from the owner name and group
        """
        result = self.couchdb.loadView("ACDC", 'owner_name_group',
             {'startkey' :[self['name'], self['group']],
               'endkey' : [self['name'], self['group']]}, []
            )
        if len(result['rows']) == 0:
            doc = None
        else:
            doc = result['rows'][0]['value']
            print "getOwnerId>>>>>>>", doc
            self['owner_id'] = doc[u'_id']
        return
        
    @connectToCouch
    def get(self):
        """
        _get_
        
        Get the owner information from the couch DB using the ID if present, or else using the 
        name and group
        
        """
        doc = None
        if self['owner_id'] == None:
            self.getOwnerId()
        try:
            doc = self.couchdb.document(self['owner_id'])
        except CMSCouch.CouchNotFoundError as ex:
            doc = None
        

        if doc == None:
            return None
        
        self.unpackDoc(doc)
        
        return self
    
    @connectToCouch
    def drop(self):
        """
        _drop_
        
        Drop this owner from the couch DB
        
        Mostly to be used for testing... 
        Would need a cascade-like iteration through collections and filesets belonging to this owner, thats implemented
        in the CouchService. This just cleans up the owner.
        Dumb use of this method could lead to a pile of collections and filesets with no owners.
        
        TODO: Maybe add a check that the delete fails if the owner has collections and filesets?
        
        """
        self.get()
        if self['owner_id'] == None:
            msg = "Document %s does not exist" % self['owner_id']
            raise RuntimeError, msg
        self.couchdb.delete_doc(self['owner_id'])
        
        

    def unpackDoc(self, doc):
        """
        _unpackDoc_
        
        Util to unpack 
        """
        leaveAlone = [ 'owner_id','database', 'url']
        [self.__setitem__(str(k), str(v)) for k,v in doc[u'owner'].items() if k not in leaveAlone ]
        self['owner_id'] = str(doc[u'_id'])
 
class CouchOwnerTests(unittest.TestCase):
    def setUp(self):
        self.url = "127.0.0.1:5984"
        self.database = "mytest2"
        
    def testA(self):
        
        
        owner = CouchOwner(name = "evansde", group = "DMWM", database = self.database, url = self.url )

        owner.create()
        
        print owner.get()
        
        owner.drop()
        
        
        
        
        
        

if __name__ == '__main__':
    unittest.main()