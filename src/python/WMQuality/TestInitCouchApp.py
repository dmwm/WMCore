#!/usr/bin/env python
# encoding: utf-8
"""
TestInitCouchApp.py

Specialisation of TestInit for Tests that require a couch connection.

Own thing to avoid making entire Test stack depend on code from couchapp

Created by Dave Evans on 2010-08-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import os
from couchapp.commands import push as couchapppush
from couchapp.config import Config
from WMCore.Database.CMSCouch import CouchServer

from WMQuality.TestInit import TestInit

class CouchAppTestHarness:
    """
    Test Harness for installing a couch database instance with several couchapps
    in a unittest.setUp and wiping it out in a unittest.tearDown
    
    
    """
    def __init__(self, dbName, couchUrl = None):
        self.couchUrl = os.environ.get("COUCHURL", couchUrl)
        self.dbName = dbName
        if self.couchUrl == None:
            msg = "COUCHRURL env var not set..."
            raise RuntimeError, msg
        self.couchServer = CouchServer(self.couchUrl)
        self.couchappConfig = Config()


    def create(self):
        """create couch db instance"""
        if self.dbName in self.couchServer.listDatabases():
            msg = "Database already exists in couch instance. bailing..."
            raise RuntimeError, msg

        self.couchServer.createDatabase(self.dbName)

    def drop(self):
        """blow away the couch db instance"""
        self.couchServer.deleteDatabase(self.dbName)

    def pushCouchapps(self, *couchappdirs):
        """
        push a list of couchapps to the database
        """
        for couchappdir in  couchappdirs:
            couchapppush(self.couchappConfig, couchappdir, "%s/%s" % (self.couchUrl, self.dbName))




class TestInitCouchApp(TestInit):
    """
    TestInit with baked in Couch goodness
    """
    
    def __init__(self, testClassName):
        TestInit.__init__(self, testClassName)
        self.couch = None

        
    def setupCouch(self, dbName,  *couchapps):
        """
        _setupCouch_
        
        Call in the setUp of your test to build a couch instance with the dbname provided
        and the required list of couchapps from WMCore/src/couchapps
        """
        self.couch = CouchAppTestHarness(dbName)
        self.couch.create()
        wmBase = self.init.getWMBASE()
        self.couch.pushCouchapps(*["%s/src/couchapps/%s" % (wmBase, couchapp) for couchapp in couchapps ])
        

    couchUrl = property(lambda x: x.couch.couchUrl)
    couchDbName = property(lambda x: x.couch.dbName)
        
    def tearDownCouch(self):
        """
        _tearDownCouch_
        
        call this in tearDown to erase all evidence of your couch misdemeanours
        """
        if self.couch != None:
            self.couch.drop()
            self.couch = None
    