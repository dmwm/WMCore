#!/usr/bin/env python
"""
CouchCollection.py

Created by Dave Evans on 2010-03-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from WMCore.ACDC.Collection import Collection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.Database.CouchUtils import connectToCouch


class CouchCollection(Collection):
    """
    Collection that can be stored in CouchDB.

    Required Args:
      database - CouchDB database instance name
      url - CouchDB Server URL
      name - name of the collection
    """

    def __init__(self, **options):
        Collection.__init__(self, **options)
        self.url = options.get("url")
        self.database = options.get("database")
        self.name = options.get("name")
        self.server = None
        self.couchdb = None

    @connectToCouch
    def drop(self):
        """
        _drop_

        Drop this collection and all files and filesets within it.
        """
        params = {"startkey": [self.name],
                  "endkey": [self.name, {}],
                  "reduce": False}
        result = self.couchdb.loadView("ACDC", "coll_fileset_docs",
                                       params)

        for row in result["rows"]:
            self.couchdb.delete_doc(row["id"])
        return

    @connectToCouch
    def populate(self):
        """
        _populate_

        The load the collection and all filesets and files out of couch.
        """
        params = {"startkey": [self.name],
                  "endkey": [self.name, {}],
                  "reduce": True, "group_level": 2}
        result = self.couchdb.loadView("ACDC", "coll_fileset_docs",
                                       params)
        self["filesets"] = []
        for row in result["rows"]:
            fileset = CouchFileset(database=self.database, url=self.url,
                                   name=row["key"][1])
            self.addFileset(fileset)
            fileset.populate()
        return
