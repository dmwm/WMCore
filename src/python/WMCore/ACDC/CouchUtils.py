#!/usr/bin/env python
# encoding: utf-8
"""
CouchUtils.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
import WMCore.Database.CMSCouch as CMSCouch

class CouchConnectionError(Exception):
    """docstring for CouchConnectionError"""
    def __init__(self, arg):
        super(CouchConnectionError, self).__init__()
        self.arg = arg
        



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
    except Exception as e:
        msg = "Exception instantiating couch services for :\n"
        msg += " url = %s\n database = %s\n" % (objectRef.url, objectRef.database)
        msg += " Exception: %s" % str(e)
        raise CouchConnectionError(msg)
        
def connectToCouch(funcRef):
    """
    _connectToCouch_
    
    Decorator method to connect the function's class object to couch
    """
    def wrapper(self, *args, **opts):
        initialiseCouch(self)
        return funcRef(self, *args, **opts)
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
    
def requireFilesetId(func):
    """
    _requireFilesetId_
    
    Decorator to require that a fileset has a fileset_id that is not None
    
    """
    def wrapper(self, *args, **opts):
        if self['fileset_id'] == None:
            self.getFilesetId()
        return func(self, *args, **opts)
    return wrapper
    
def requireOwnerId(func):
    """
    _requireOwnerId_

    Decorator to require that a fileset has a owner_id that is not None

    """
    def wrapper(self, *args, **opts):
        if self['owner_id'] == None:
            self.getOwnerId()
        return func(self, *args, **opts)
    return wrapper