#!/usr/bin/env python
# encoding: utf-8
"""
User.py

Created by Dave Evans on 2010-07-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from WMCore.GroupUser.CouchObject import CouchObject
import WMCore.GroupUser.Decorators as Decorators
from WMCore.GroupUser.Group import Group



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

        retval = self.couch.commitOne(document)
        document["_id"] = retval[0]["id"]
        document["_rev"] = retval[0]["rev"]
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
