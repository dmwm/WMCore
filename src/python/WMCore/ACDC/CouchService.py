#!/usr/bin/env python
# encoding: utf-8
"""
CouchService.py

Created by Dave Evans on 2010-04-20.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from builtins import object
from time import time

import WMCore.Database.CouchUtils as CouchUtils


class CouchService(object):
    def __init__(self, **options):
        super(CouchService, self).__init__()
        self.options = {}
        self.options.update(options)
        self.url = options.get('url', None)
        self.database = options.get('database', None)
        self.server = None
        self.couchdb = None

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
        result = self.couchdb.loadView("ACDC", "byCollectionName", options={"reduce": False}, keys=[collectionName])
        for entry in result["rows"]:
            self.couchdb.queueDelete(entry["value"])
        return self.couchdb.commit()

    @CouchUtils.connectToCouch
    def removeOldFilesets(self, expirationDays):
        """
        _removeOldFilesets_

        Remove filesets older than certain date defined
        in expirationDays (in days).
        """
        cutoutPoint = time() - (expirationDays * 3600 * 24)
        result = self.couchdb.loadView("ACDC", "byTimestamp", {"endkey": cutoutPoint})
        count = 0
        for entry in result["rows"]:
            self.couchdb.queueDelete(entry["value"])
            count += 1
        self.couchdb.commit()
        return count

    @CouchUtils.connectToCouch
    def listCollectionNames(self):
        options = {'reduce': True, 'group_level': 1, 'stale': "update_after"}
        result = self.couchdb.loadView("ACDC", "byCollectionName", options)
        collectionNames = []
        for row in result["rows"]:
            collectionNames.append(row["key"])
        return collectionNames
