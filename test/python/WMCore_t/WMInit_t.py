#!/usr/bin/env python
# encoding: utf-8
"""
WMInit_t.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""
import os
import unittest
import threading


from WMCore.WMInit import WMInit, getWMBASE

class WMInit_t(unittest.TestCase):

    def setUp(self):
        self.testDB = 'unittest_%s' % self.__class__.__name__
        self.init = WMInit()
        url     = os.environ.get("DATABASE")
        dialect = os.environ.get("DIALECT", "mysql")
        sock    = os.environ.get("DBSOCK", None)
        self.init.setDatabaseConnection(url, dialect, sock)
        self.init.destroyDatabase(self.testDB)
        self.init.createDatabase(self.testDB)

    def tierDown(self):
        self.init.destroyDatabase(self.testDB)

    def testA(self):

        try:
            getWMBASE()
        except:
            self.fail("Error calling WMInit.getWMBASE")


    def testB_Database(self):
        """
        _Database_

        Testing the database stuff.
        """
        self.init.createDatabase(self.testDB)
        self.init.destroyDatabase(self.testDB)

        self.init.createDatabase(self.testDB)
        myThread = threading.currentThread()
        fount = False
        for row in myThread.dbi.processData("SHOW DATABASES"):
            dbs = [r[0] for r in row.fetchall()]
            if self.testDB in dbs:
                found = True
        self.assertEqual(found, True)
        self.init.destroyDatabase(self.testDB)

        self.init.createDatabase(self.testDB)
        self.init.setSchema(modules = ['WMCore.WMBS'])
        myThread = threading.currentThread()
        fount = False
        for row in myThread.dbi.processData("SHOW DATABASES"):
            dbs = [r[0] for r in row.fetchall()]
            if self.testDB in dbs:
                found = True
        self.assertEqual(found, True)
        myThread.transaction.begin()
        myThread.transaction.commit()
        self.init.destroyDatabase(self.testDB)

if __name__ == '__main__':
    unittest.main()
