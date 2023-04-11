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
import logging

from couchapp.commands import push as couchapppush
from WMCore.Database.CMSCouch import CouchServer

from WMQuality.TestInit import TestInit


class CouchAppTestHarness(object):
    """
    Test Harness for installing a couch database instance with several couchapps
    in a unittest.setUp and wiping it out in a unittest.tearDown


    """

    def __init__(self, couchUrl=None, testClassName=None):
        self.couchUrl = os.environ.get("COUCHURL", couchUrl)
        if self.couchUrl == None:
            msg = "COUCHRURL env var not set..."
            raise RuntimeError(msg)
        if self.couchUrl.endswith('/'):
            raise RuntimeError("COUCHURL env var shouldn't end with /")
        self.couchServer = CouchServer(self.couchUrl)
        self.testClassName = testClassName
        self.logger = logging.getLogger()

    def create(self, dbName, dropExistingDb=True):
        """
        create couch db instance

        :param dbName: database name to create
        :param dropExistingDb: flag to drop existing database name if it exists
        """
        # import pdb
        # pdb.set_trace()
        databases = self.couchServer.listDatabases()
        msg = f"Create {dbName} in {self.couchUrl}, existing DBs={databases}"
        self.logger.info(msg)
        if dbName in databases:
            if not dropExistingDb:
                return
            self.drop(dbName)

        self.couchServer.createDatabase(dbName)

    def drop(self, dbName):
        """
        drop given database name from CouchDB

        :param dbName: database name
        """
        databases = self.couchServer.listDatabases()
        msg = f"Drop {dbName} in {self.couchUrl}, existing DBs={databases}"
        self.logger.info(msg)
        if dbName in databases:
            self.couchServer.deleteDatabase(dbName)

    def pushCouchapps(self, dbName, *couchappdirs):
        """
        push a list of couchapps to the given database

        :param dbName: database name
        :param couchappdirs: list of couch applications
        """
        for couchappdir in couchappdirs:
            msg = f"Push {couchappdir} in {self.couchUrl} database {dbName}"
            self.logger.info(msg)
            couchapppush(couchappdir, "%s/%s" % (self.couchUrl, urllib.parse.quote_plus(dbName)))


class TestInitCouchApp(TestInit):
    """
    TestInit with baked in Couch goodness
    """

    def __init__(self, testClassName, dropExistingDb=True):
        TestInit.__init__(self, testClassName)
        self.databases = []
        self.couch = CouchAppTestHarness(testClassName=testClassName)
        # for experiments, after tests run, it's useful to have CouchDB
        # populated with the testing data - having tearDownCouch commented
        # out, this flag prevents from re-initializing the database
        self.dropExistingDb = dropExistingDb
        self.couchUrl = self.couch.couchUrl
        self.couchDbName = "Not set yet"

    def couchAppRoot(self, couchapp):
        """
        Return parent path containing couchapp

        :param couchapp: couch application to use
        """
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

        :param dbName: database name
        :param *couchapps: list of couch apps associated with given database
        """
        # this function performs two set of actions:
        # 1. it creates given dbName
        # 2. it registter given set of couchapps in that DB
        # since those are independent operations we separate them here to avoid
        # racing conditions on multiple setupCouch() calls with the same dbName but different apps, e.g.
        # see test/python/WMCore_t/WMSpec_t/StdSpecs_t/Resubmission_t.py
        #     self.testInit.setupCouch("resubmission_t", "ReqMgr")
        #     self.testInit.setupCouch("resubmission_t", "ConfigCache")
        if dbName not in self.databases:
            self.databases.append(dbName)
            self.couch.create(dbName, dropExistingDb=self.dropExistingDb)
        # just create the db is couchapps are not specified
        if len(couchapps) > 0:
            self.couch.pushCouchapps(dbName, *[os.path.join(self.couchAppRoot(couchapp), couchapp) for couchapp in couchapps])
        self.couchDbName = dbName

    def tearDownCouch(self):
        """
        _tearDownCouch_

        call this in tearDown to erase all evidence of your couch misdemeanours
        """
        for database in self.databases:
            self.couch.drop(database)
