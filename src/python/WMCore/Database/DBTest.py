#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : DBTest.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Base class for WMCore database related unit tests.
It defines test database name based on user preferences specified via
environment variables and setup logic to setup/tear down actions
based on database. It relies on the following environement variables
DATABASE is used when no dbUrl is passed to ctor
WMCORE_TEST_DATABASE is used to read db name for the test, it not set
unittest_<className> will be used instead.
WMCORE_TEST_DATABASE_DELETE is used to identify if client want
to make clean setup, i.e. delete content of previously used database
"""
from __future__ import print_function, division

# system modules
import os
import time
import logging
import threading

from sqlalchemy.exc import OperationalError

# CMS modules
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMException import WMException
from WMCore.Database.Transaction import Transaction

def closeTransactions():
    """close open transactions"""
    myThread = threading.currentThread()
    if hasattr(myThread, 'transaction') and getattr(myThread.transaction, 'transaction', None):
        try:
            myThread.transaction.commit()
        except:
            try:
                myThread.transaction.rollback()
            except:
                pass

class DBTest(object):
    def __init__(self, clsName, dbUrl=None, socket=None):
        myThread = threading.currentThread()
        self.logger = myThread.logger if hasattr(myThread, "logger") else logging.getLogger()
        options = {}
        if not dbUrl:
            dbUrl = os.getenv('DATABASE', None)
        if socket and dbUrl.find('unix_socket') == -1:
            dbUrl = '%s?unix_socket=%s' % (dbUrl, socket)
        if not hasattr(myThread, 'dialect'):
            dialect = dbUrl.split('://')[0]
            if dialect.lower() == 'mysql':
                dialect = 'MySQL'
            elif dialect.lower() == 'oracle':
                dialect = 'Oracle'
            elif dialect.lower() == 'http':
                dialect = 'CouchDB'
            else:
                msg = "Unsupported dialect %s" % dialect
                self.logger.error(msg)
                raise Exception(msg)
            myThread.dialect = dialect
        dbFactory = DBFactory(self.logger, dbUrl, options)
        self.dbi = dbFactory.connect()
        if not clsName:
            clsName = '%s_%s' % (self.__class__.__name__, int(time.time()))
        self.dbName = os.environ.get('WMCORE_TEST_DATABASE', 'unittest_%s' % abs(hash(clsName)))
        if not hasattr(myThread, 'dbi'):
            myThread.dbi = self.dbi
        if not hasattr(myThread, 'transaction'):
            myThread.transaction = Transaction(self.dbi)
        if not hasattr(myThread, 'logger'):
            myThread.logger = self.logger
        print("DBTest dialect=%s dbUrl=%s socket=%s dbName=%s" \
                % (myThread.dialect, dbUrl, socket, self.dbName))
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

    def createDatabase(self, dbName=None):
        """Create test database"""
        if not dbName:
            dbName = self.dbName
        closeTransactions()
        daoFactory = DAOFactory(package = "WMCore.Database",
                                logger = self.logger,
                                dbinterface = self.dbi)
        dao = daoFactory(classname = "CreateDatabase")
        try:
            dao.execute(dbName=dbName)
        except Exception as ex:
            msg =  "Critical error while attempting to create database=%s\n" % dbName
            msg += str(ex)
            self.logger.error(msg)
            raise WMException(msg)

    def deleteDatabase(self, dbName=None):
        """Delete test database"""
        closeTransactions()
        if not dbName:
            dbName = self.dbName
        daoFactory = DAOFactory(package = "WMCore.Database",
                                logger = self.logger,
                                dbinterface = self.dbi)
        dao = daoFactory(classname = "DeleteDatabase")
        try:
            dao.execute(dbName)
        except OperationalError:
            pass # it happens when database does not exists
        except Exception as ex:
            msg =  "Critical error while attempting to delete database=%s\n" % dbName
            msg += str(ex)
            self.logger.error(msg)
            raise WMException(msg)
