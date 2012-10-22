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

        init = WMInit()
        url     = os.environ.get("DATABASE")
        dialect = os.environ.get("DIALECT")
        sock    = os.environ.get("DBSOCK", None)

        init.setDatabaseConnection(url, dialect, sock)

        try:
            # Initial clear should work
            myThread = threading.currentThread()
            init.clearDatabase()

            # Clear one after another should work
            init.setSchema(modules = ['WMCore.WMBS'])
            init.clearDatabase()
            init.setSchema(modules = ['WMCore.WMBS'])
            init.clearDatabase()

            # Clear non-existant DB should work
            # Drop the database, and then make sure the database gets recreated
            a = myThread.dbi.engine.url.database
            dbName = myThread.dbi.processData("SELECT DATABASE() AS dbname")[0].fetchall()[0][0]
            myThread.dbi.processData("DROP DATABASE %s" % dbName)
            dbName = myThread.dbi.processData("SELECT DATABASE() AS dbname")[0].fetchall()[0][0]
            self.assertEqual(dbName, None)
            init.clearDatabase()
            dbName = myThread.dbi.processData("SELECT DATABASE() AS dbname")[0].fetchall()[0][0]
            self.assertEqual(dbName, a)


            init.setSchema(modules = ['WMCore.WMBS'])
            myThread.transaction.begin()
            myThread.transaction.processData("SELECT * FROM wmbs_job")
            init.clearDatabase()
            dbName = myThread.dbi.processData("SELECT DATABASE() AS dbname")[0].fetchall()[0][0]
            self.assertEqual(dbName, a)
            myThread.transaction.begin()
            init.setSchema(modules = ['WMCore.WMBS'])
            myThread.transaction.commit()
        except:
            init.clearDatabase()
            raise

        init.clearDatabase()

        return

if __name__ == '__main__':
    unittest.main()
