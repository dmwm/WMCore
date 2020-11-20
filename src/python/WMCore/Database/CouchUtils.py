#!/usr/bin/env python
# encoding: utf-8
"""
CouchUtils.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from __future__ import print_function

from future import standard_library
standard_library.install_aliases()

from http.client import HTTPException

import functools

import WMCore.Database.CMSCouch as CMSCouch


class CouchConnectionError(Exception):
    """docstring for CouchConnectionError"""
    def __init__(self, arg):
        super(CouchConnectionError, self).__init__(arg)




def initialiseCouch(objectRef):
    if objectRef.couchdb != None:
        return
    if objectRef.url == None:
        msg = "url for couch service not provided"
        raise CouchConnectionError(msg)
    if objectRef.database == None:
        msg = "database name for couch service not provided"
        raise CouchConnectionError(msg)
    try:
        objectRef.server = CMSCouch.CouchServer(objectRef.url)
        objectRef.couchdb = objectRef.server.connectDatabase(objectRef.database)
    except HTTPException as e:
        msg = "%s with status: %s, reason: %s and result: %s" % (repr(e),
                                                                 getattr(e, 'status', ""),
                                                                 getattr(e, 'reason', ""),
                                                                 getattr(e, 'result', ""))
        raise CouchConnectionError(msg)
    except Exception as e:
        msg = "Exception instantiating couch services for :\n"
        msg += " url = %s\n database = %s\n" % (objectRef.url, objectRef.database)
        msg += " Exception: %s" % str(e)
        print(msg)
        raise CouchConnectionError(msg)

def connectToCouch(funcRef):
    """
    _connectToCouch_

    Decorator method to connect the function's class object to couch
    """
    @functools.wraps(funcRef)
    def wrapper(x, *args, **opts):
        initialiseCouch(x)
        return funcRef(x, *args, **opts)
    return wrapper

def requireOwner(func):
    """
    _requireOwner_

    Decorator to ensure that the owner attribute of a couch ACDC object is not None
    """

    def wrapper(self, *args, **opts):
        if self.owner == None:
            msg = "Owner not provided for Collection"
            raise RuntimeError(msg)
        return func(self, *args, **opts)
    return wrapper

def requireCollection(func):
    """
    _requireCollection_

    Decorator to ensure that the collection attribute of a couch ACDC object is not None
    """

    def wrapper(self, *args, **opts):
        if self.collection == None:
            msg = "Collection not provided for Collection"
            raise RuntimeError(msg)
        return func(self, *args, **opts)
    return wrapper

def requireFilesetName(func):
    """
    _requireFilesetName_

    Decorator to require that a fileset has a name that is not None

    """
    def wrapper(self, *args, **opts):
        if not 'name' in self or self['name'] == None:
            raise RuntimeError("Filesets must be named")
        return func(self, *args, **opts)
    return wrapper

def requireCollectionName(func):
    """
    _requireCollectionName_

    Decorator to require that a collection has a name that is not None

    """
    def wrapper(self, *args, **opts):
        if not 'name' in self or self['name'] == None:
            raise RuntimeError("Filesets must be named")
        return func(self, *args, **opts)
    return wrapper
