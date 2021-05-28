#!/usr/bin/env python
# encoding: utf-8
"""
TestInitCouchApp.py

Specialisation of TestInit for Tests that require a couch connection.

Own thing to avoid making entire Test stack depend on code from couchapp

Created by Dave Evans on 2010-08-19.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import object
from future import standard_library

standard_library.install_aliases()

import os
import urllib.parse

from couchapp.commands import push as couchapppush
from WMCore.Database.CMSCouch import CouchServer

from WMQuality.TestInit import TestInit


class CouchAppTestHarness(object):
    """
    Test Harness for installing a couch database instance with several couchapps
    in a unittest.setUp and wiping it out in a unittest.tearDown


    """

    def __init__(self, dbName, couchUrl=None):
        self.couchUrl = os.environ.get("COUCHURL", couchUrl)
        self.dbName = dbName
        if self.couchUrl == None:
            msg = "COUCHRURL env var not set..."
            raise RuntimeError(msg)
        if self.couchUrl.endswith('/'):
            raise RuntimeError("COUCHURL env var shouldn't end with /")
        self.couchServer = CouchServer(self.couchUrl)

    def create(self, dropExistingDb=True):
        """create couch db instance"""
        # import pdb
        # pdb.set_trace()
        if self.dbName in self.couchServer.listDatabases():
            if not dropExistingDb:
                return
            self.drop()

        self.couchServer.createDatabase(self.dbName)

    def drop(self):
        """blow away the couch db instance"""
        self.couchServer.deleteDatabase(self.dbName)

    def pushCouchapps(self, *couchappdirs):
        """
        push a list of couchapps to the database
        """
        for couchappdir in couchappdirs:
            couchapppush(couchappdir, "%s/%s" % (self.couchUrl, urllib.parse.quote_plus(self.dbName)))


class TestInitCouchApp(TestInit):
    """
    TestInit with baked in Couch goodness
    """

    def __init__(self, testClassName, dropExistingDb=True):
        TestInit.__init__(self, testClassName)
        self.databases = []
        self.couch = None
        # for experiments, after tests run, it's useful to have CouchDB
        # populated with the testing data - having tearDownCouch commented
        # out, this flag prevents from re-initializing the database
        self.dropExistingDb = dropExistingDb

    def couchAppRoot(self, couchapp):
        """Return parent path containing couchapp"""
        wmcoreroot = os.path.normpath(os.path.join(self.init.getWMBASE(), '..', '..', '..'))
        develPath = os.path.join(self.init.getWMBASE(), "src", "couchapps")
        if os.path.exists(os.path.join(develPath, couchapp)):
            return develPath
        elif os.path.exists(os.path.join(wmcoreroot, 'xdata', 'couchapps', couchapp)):
            return os.path.join(wmcoreroot, 'xdata', 'couchapps')
        elif os.path.exists(os.path.join(wmcoreroot, 'data', 'couchapps', couchapp)):
            return os.path.join(wmcoreroot, 'data', 'couchapps')
        raise OSError('Cannot find couchapp: %s' % couchapp)

    def setupCouch(self, dbName, *couchapps):
        """
        _setupCouch_

        Call in the setUp of your test to build a couch instance with the dbname provided
        and the required list of couchapps from WMCore/src/couchapps
        """
        self.databases.append(dbName)
        self.couch = CouchAppTestHarness(dbName)
        self.couch.create(dropExistingDb=self.dropExistingDb)
        # just create the db is couchapps are not specified
        if len(couchapps) > 0:
            self.couch.pushCouchapps(*[os.path.join(self.couchAppRoot(couchapp), couchapp) for couchapp in couchapps])

    couchUrl = property(lambda x: x.couch.couchUrl)
    couchDbName = property(lambda x: x.couch.dbName)

    def tearDownCouch(self):
        """
        _tearDownCouch_

        call this in tearDown to erase all evidence of your couch misdemeanours
        """
        for database in self.databases:
            couch = CouchAppTestHarness(database)
            couch.drop()

        self.couch = None
        return
