#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : DBTest.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Base class for WMCore database related unit tests.
             It defines test database name based on user preferences specified via
             environment variables and setup logic to setup/tear down actions
             based on database.
"""
from __future__ import print_function, divide

# system modules
import os
import unittest

class DBTest(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(DBTest, self).__init__(methodName)
        self.dbName = os.environ.get('WMCORE_TEST_DATABASE', \
                'unittest_%s' % self.__class__.__name__)
        print("WMCoreTestDB test %s database" % self.dbName)
        enforce = os.environ.get('WMCORE_TEST_DATABASE_DELETE', False)
        if  enforce:
            print("WMCORE_TEST_DATABASE_DELETE=%s, will delete %s" % (enforce, self.dbName))
            self.deleteDatabase()
            self.createDatabase()

    def setUp(self):
        """Setup database for unittests"""
        if  self.dbName.startswith('unittest'):
            self.deleteDatabase()
            self.createDatabase()

    def tearDown(self):
        """Tear down database"""
        if self.dbName.startswith('unittest'):
            self.deleteDatabase()

    def createDatabase(self):
        """Abstract method to create test database, must be implemented in sub-classes"""
        print("Create %s" % self.dbName)

    def deleteDatabase(self):
        """Abstract method to delete test database, must be implemented in sub-classes"""
        print("Delete %s" % self.dbName)
