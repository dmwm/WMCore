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
import threading

# CMS modules
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException

class DBTest(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(DBTest, self).__init__(methodName)
        self.dbName = os.environ.get('WMCORE_TEST_DATABASE', \
                'unittest_%s' % self.__class__.__name__)
        print("DBTest use %s database" % self.dbName)
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

    def createDatabase(self, dbName):
        """Create test database"""
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        dao = daoFactory(classname = "CreateDatabase")
        try:
            dao.execute(dbName=dbname)
        except Exception as ex:
            msg =  "Critical error while attempting to create database=%s\n" % dbName
            msg += str(ex)
            myThread.logger.error(msg)
            raise WMInitException(msg)

    def deleteDatabase(self, dbname=None, modules = []):
        """Delete test database"""
        myThread = threading.currentThread()
        # close open transactions
        if hasattr(myThread, 'transaction') and getattr(myThread.transaction, 'transaction', None):
            try:
                myThread.transaction.commit()
            except:
                try:
                    myThread.transaction.rollback()
                except:
                    pass

        daoFactory = DAOFactory(package = "WMCore.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        dao = daoFactory(classname = "DeleteDatabase")
        try:
            dao.execute(dbname)
        except Exception as ex:
            msg =  "Critical error while attempting to delete database=%s\n" % dbName
            msg += str(ex)
            myThread.logger.error(msg)
            raise WMInitException(msg)
