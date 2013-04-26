#!/usr/bin/env python
# encoding: utf-8
"""
CouchService.py

Created by Dave Evans on 2010-04-20.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from time import time

from WMCore.ACDC.Service import Service
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset

from WMCore.GroupUser.User import User, makeUser
import WMCore.Database.CouchUtils as CouchUtils
import WMCore.Database.CMSCouch as CMSCouch

class CouchService(Service):

    def __init__(self, **options):
        Service.__init__(self, **options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None

    @CouchUtils.connectToCouch
    def listCollections(self, owner):
        """
        _listCollections_

        List the collections belonging to an owner.
        """
        params = {"startkey": [owner.group.name, owner.name],
                  "endkey": [owner.group.name, owner.name, {}],
                  "reduce": True, "group_level": 3}

        result = self.couchdb.loadView("ACDC", "owner_coll_fileset_docs",
                                       params)

        for row in result["rows"]:
            coll = CouchCollection(name = row["key"][2],
                                   database = self.database, url = self.url)
            coll.setOwner(owner)
            coll.populate()
            yield coll

    @CouchUtils.connectToCouch
    def listOwners(self):
        """
        _listOwners_

        List the owners in the DB

        """
        result = self.couchdb.loadView("GroupUser", 'name_map', {}, [])
        users = []
        for row in result[u'rows']:
            group = row[u'key'][0]
            user = row[u'key'][1]
            users.append(makeUser(group, user, self.url, self.database))
        return users

    def newOwner(self, group, user):
        """
        _newOwner_

        Add a new owner
        """
        userInstance = makeUser(group, user, self.url, self.database)
        userInstance.create()
        return userInstance

    @CouchUtils.connectToCouch
    def removeOwner(self, owner):
        """
        _removeOwner_

        Remove an owner and all the associated collections and filesets

        """
        result = self.couchdb.loadView("GroupUser", 'owner_group_user',
             {'startkey' :[owner.group.name, owner.name],
               'endkey' : [owner.group.name, owner.name]}, []
            )
        for row in result[u'rows']:
            deleteMe = CMSCouch.Document()
            deleteMe[u'_id'] = row[u'value'][u'id']
            deleteMe[u'_rev'] = row[u'value'][u'rev']
            deleteMe.delete()
            self.couchdb.queue(deleteMe)
        self.couchdb.commit()
        owner.drop()
        return

    def listFilesets(self, collectionInstance):
        """
        _listFilesets_

        List filesets for the collection instance provided.
        """
        collectionInstance.populate()

        for fileset in collectionInstance["filesets"]:
            yield fileset

    @CouchUtils.connectToCouch
    def removeFilesetsByCollectionName(self, collectionName):
        """
        _removeFilesetsByCollectionName_

        Remove all the collections matching certain collection
        name.
        """
        result = self.couchdb.loadView("ACDC", "byCollectionName", keys = [collectionName])
        for entry in result["rows"]:
            self.couchdb.queueDelete(entry["value"])
        self.couchdb.commit()
        return

    @CouchUtils.connectToCouch
    def removeOldFilesets(self, expirationDays):
        """
        _removeOldFilesets_

        Remove filesets older than certain date defined
        in expirationDays (in days).
        """
        cutoutPoint = time() - (expirationDays * 3600 * 24)
        result = self.couchdb.loadView("ACDC", "byTimestamp", {"endkey" : cutoutPoint})
        count = 0
        for entry in result["rows"]:
            self.couchdb.queueDelete(entry["value"])
            count += 1
        self.couchdb.commit()
        return count
