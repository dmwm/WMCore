#!/usr/bin/env python
# encoding: utf-8
"""
User.py

Created by Dave Evans on 2010-07-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
from WMCore.GroupUser.CouchObject import CouchObject
import WMCore.GroupUser.Decorators as Decorators 
from WMCore.GroupUser.Group import Group
import unittest


class User(CouchObject):
    """
    _User_
    
    
    """
    def __init__(self, **options):
        CouchObject.__init__(self)
        self.cdb_document_data = "user"
        self.setdefault('name', None)
        self.setdefault('proxy', {})
        self.couch_url = None
        self.couch_db = None
        self.group = None
        self.update(options)

    document_id = property(lambda x : "user-%s" % x['name'] )
    name = property(lambda x: x['name'])
    
    def setGroup(self, groupInstance):
        """
        _setGroup_
        
        Set the group attribute of this object
        """
        self.group = groupInstance
        self['group'] = groupInstance['name']
        if groupInstance.connected:
            # if the group already has the couch instance in it, borrow those settings
            self.setCouch(groupInstance.cdb_url, groupInstance.cdb_database)
        
    @Decorators.requireConnection
    @Decorators.requireGroup
    def ownThis(self, document):
        """
        _ownThis_
        
        Given a Couch Document, insert the details of this owner in a standard
        way so that the document can be found using the standard group/user views
        Note: if doc doesnt have both _id and _rev keys this change wont stick
        """
        document['owner'] = {}
        document['owner']['user'] = self['name']
        document['owner']['group'] = self.group['name']
        self.couch.commitOne(document)
        return
        
    @Decorators.requireConnection
    @Decorators.requireGroup
    def create(self):
        """
        _create_
        
        Overide the base class create to make sure the group exists when adding the user
        (Note: Overriding wipes out decorators...)
        """
        if not self.couch.documentExists(self.group.document_id):
            # no group => create group
            self.group.create()
        # call base create to make user
        CouchObject.create(self)

#ToDo: Override drop to drop all documents owned by the owner...
    
def makeUser(groupname, username, couchUrl = None, couchDatabase = None):
    """
    _makeUser_
    
    factory like util that creates a user and group object
    
    """
    group = Group(name = groupname)
    if couchUrl != None:
        group.setCouch(couchUrl, couchDatabase)
        group.connect()
    user  = User(name = username)
    user.setGroup(group)
    return user
    


from WMCore.Database.CMSCouch import Document

class UserTest(unittest.TestCase):
    """Test Case for User"""
    def setUp(self):
        self.database = "groupuser"
        self.url = "127.0.0.1:5984"
    
    def testA(self):
        """instantiate & jsonise"""

        u1 = User(name = "evansde77")
        g1 = Group(name = "DMWM", administrators = ["evansde77", "drsm79"])
        g1.setCouch(self.url, self.database)
        g1.connect()
        u1. setGroup(g1)
        
        u1.create()
        
        u2 = User(name = "evansde77")
        u2.setCouch(self.url, self.database)
        u2.get()
        
        u1.drop()
        g1.drop()
        
        
    def testB(self):
        """test owning some sample documents"""
        
        u1 = User(name = "evansde77")
        g1 = Group(name = "DMWM", administrators = ["evansde77", "drsm79"])
        g1.setCouch(self.url, self.database)
        g1.connect()
        u1.setGroup(g1)
        u1.create()

        doc1 = Document()
        doc1['test-data'] = {"key1" : "value1"}
        doc2 = Document()
        doc2['test-data'] = {"key2" : "value2"} 
        id1 = g1.couch.commitOne(doc1)[0]
        id2 = g1.couch.commitOne(doc2)[0]
        doc1['_id'] = id1[u'id']
        doc1['_rev'] = id1[u'rev']
        doc2['_id'] = id2[u'id']
        doc2['_rev'] = id2[u'rev']        

        u1.ownThis(doc1)
        u1.ownThis(doc2)

        self.failUnless(doc1.has_key("owner"))
        self.failUnless(doc2.has_key("owner"))
        self.failUnless(doc1['owner'].has_key('user'))
        self.failUnless(doc1['owner'].has_key('group'))
        self.failUnless(doc1['owner']['user'] == u1['name'])
        self.failUnless(doc1['owner']['group'] == u1['group'])
        self.failUnless(doc2['owner'].has_key('user'))
        self.failUnless(doc2['owner'].has_key('group'))
        self.failUnless(doc2['owner']['user'] == u1['name'])
        self.failUnless(doc2['owner']['group'] == u1['group'])
        
                
        #g1.couch.delete_doc(id1[u'id'])
        #g1.couch.delete_doc(id2[u'id'])    
        
        
if __name__ == '__main__':
    unittest.main()