#!/usr/bin/env python
# encoding: utf-8
"""
CouchObject.py

Created by Dave Evans on 2010-07-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from __future__ import print_function

from builtins import str
import json as jsonlib

import WMCore.Database.CMSCouch as CMSCouch
import WMCore.GroupUser.Decorators as Decorators


class CouchConnectionError(Exception):
    """docstring for CouchConnectionError"""

    def __init__(self, arg):
        super(CouchConnectionError, self).__init__()
        self.arg = arg


class CouchObject(dict):
    """
    Base class for dictionary derived couch documents for this package.
    (May even be generally useful...)

    Essentially a specialised dict class that has attributes needed to talk to Couch
    via the CMSCouch API.

    This class is expected to be overridden and the derived class must override the document_id
    property to generate the document id from the data within the dictionary.
    This class assumes some simple mapping is used to generate the document ID as a property, rather
    on doing anything too fancy
    """

    def __init__(self):
        dict.__init__(self)
        self.couch = None
        self.cdb_server = None
        self.cdb_database = None
        self.cdb_url = None
        self.cdb_document_id = None
        self.cdb_document_data = "CouchObject"

    connected = property(lambda x: x.couch is not None)
    json = property(lambda x: jsonlib.dumps(dict(x)))
    # doc_id default returns None, which signals use couch-generated doc id
    document_id = property(lambda x: None)

    def setCouch(self, url, database):
        """
        _setCouch_

        Set the contant info for the couch database
        """
        self.cdb_url = url
        self.cdb_database = database

    def connect(self):
        """
        _connect_

        Initialise the couch database connection for this object.
        This gets called automagically by the requireConnected decorator
        """
        if self.connected:
            return
        if self.cdb_url is None:
            msg = "url for couch service not provided"
            raise CouchConnectionError(msg)
        if self.cdb_database is None:
            msg = "database name for couch service not provided"
            raise CouchConnectionError(msg)
        try:
            self.cdb_server = CMSCouch.CouchServer(self.cdb_url)
            self.couch = self.cdb_server.connectDatabase(self.cdb_database)
        except Exception as ex:
            msg = "Exception instantiating couch services for :\n"
            msg += " url = %s\n database = %s\n" % (self.cdb_url, self.cdb_database)
            msg += " Exception: %s" % str(ex)
            print(msg)
            raise CouchConnectionError(msg)

    @Decorators.requireConnection
    def create(self):
        """
        _create_

        Create the couch document for this object.
        """
        if not self.couch.documentExists(self.document_id):
            couchDoc = CMSCouch.Document(self.document_id, {self.cdb_document_data: dict(self)})
            self.couch.commitOne(couchDoc)

    @Decorators.requireConnection
    def get(self):
        """
        _get_

        given the doc_id generated from the derived class, get the document and update
        the data in the dictionary from it

        """
        if not self.couch.documentExists(self.document_id):
            raise RuntimeError("Document: %s not found" % self.document_id)

        doc = self.couch.document(self.document_id)
        data = doc.get(self.cdb_document_data, {})
        dict.update(self, data)
        return

    @Decorators.requireConnection
    def drop(self):
        """
        _drop_

        remove the doc representing this object, possibly with a chained wipeout
        of all other docs referencing it by owner or group or whatever, if thats
        needed, override in the classes derived from this
        """
        if not self.couch.documentExists(self.document_id):
            raise RuntimeError("Document: %s not found" % self.document_id)
        self.couch.delete_doc(self.document_id)
