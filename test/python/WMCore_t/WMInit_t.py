#!/usr/bin/env python
# encoding: utf-8
"""
WMInit_t.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""
from __future__ import print_function

import os
import threading
import unittest

from WMCore.WMInit import WMInit, getWMBASE


class WMInit_t(unittest.TestCase):
    def testA(self):

        try:
            getWMBASE()
        except:
            self.fail("Error calling WMInit.getWMBASE")

    def testMySQLDatabase(self):
        """
        Testing MySQL basic operations
        """
        dialect = os.environ.get("DIALECT", "MySQL")
        if dialect.lower() == 'oracle':
            # this test can only run for MySQL
            return

        init = WMInit()
        url = os.environ.get("DATABASE")
        sock = os.environ.get("DBSOCK", None)
        init.setDatabaseConnection(url, dialect, sock)

        selectDbName = "SELECT DATABASE() AS dbname"
        destroyDbName = "DROP DATABASE %s"

        try:
            # Initial clear should work
            myThread = threading.currentThread()
            init.clearDatabase()

            # Clear non-existant DB should work
            init.clearDatabase()

            init.setSchema(modules=['WMCore.WMBS'])

            # Drop the database, and then make sure the database gets recreated
            a = myThread.dbi.engine.url.database
            self.assertEqual(myThread.dbi.engine.name, "mysql")
            self.assertTrue(myThread.dbi.engine.url.database in ("wmcore_unittest", "WMCore_unit_test"))
            self.assertEqual(myThread.dbi.engine.url.get_backend_name(), "mysql")
            self.assertEqual(myThread.dbi.engine.url.get_driver_name(), "mysqldb")
            self.assertEqual(myThread.dbi.engine.url.host, "localhost")

            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertEqual(dbName, a)
            myThread.dbi.processData(destroyDbName % dbName)
            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertEqual(dbName, None)
            init.clearDatabase()
            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertEqual(dbName, a)

            init.setSchema(modules=['WMCore.WMBS'])
            myThread.transaction.begin()
            myThread.transaction.processData("SELECT * FROM wmbs_job")
            init.clearDatabase()
            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertEqual(dbName, a)
            myThread.transaction.begin()
            init.setSchema(modules=['WMCore.WMBS'])
            myThread.transaction.commit()
        except:
            init.clearDatabase()
            raise
        else:
            init.clearDatabase()

        return

    def testOracleDatabase(self):
        """
        Testing Oracle basic operations
        """
        dialect = os.environ.get("DIALECT", "MySQL")
        if dialect.lower() == 'mysql':
            # this test can only run for Oracle
            return

        init = WMInit()
        url = os.environ.get("DATABASE")
        init.setDatabaseConnection(url, dialect)

        selectDbName = "SELECT ora_database_name FROM DUAL"
        destroyDb = """DECLARE
                 BEGIN

                   execute immediate 'purge recyclebin';

                   -- Tables
                   FOR o IN (SELECT table_name name FROM user_tables) LOOP
                     execute immediate 'drop table ' || o.name || ' cascade constraints';
                   END LOOP;

                   -- Sequences
                   FOR o IN (SELECT sequence_name name FROM user_sequences) LOOP
                     execute immediate 'drop sequence ' || o.name;
                   END LOOP;

                   -- Triggers
                   FOR o IN (SELECT trigger_name name FROM user_triggers) LOOP
                     execute immediate 'drop trigger ' || o.name;
                   END LOOP;

                   -- Synonyms
                   FOR o IN (SELECT synonym_name name FROM user_synonyms) LOOP
                     execute immediate 'drop synonym ' || o.name;
                   END LOOP;

                   -- Functions
                   FOR o IN (SELECT object_name name FROM user_objects WHERE object_type = 'FUNCTION') LOOP
                     execute immediate 'drop function ' || o.name;
                   END LOOP;

                   -- Procedures
                   FOR o IN (SELECT object_name name FROM user_objects WHERE object_type = 'PROCEDURE') LOOP
                     execute immediate 'drop procedure ' || o.name;
                   END LOOP;

                   execute immediate 'purge recyclebin';

                 END;"""

        try:
            # Initial clear should work
            myThread = threading.currentThread()
            init.clearDatabase()

            # Clear non-existant DB should work
            init.clearDatabase()

            init.setSchema(modules=['WMCore.WMBS'])

            # Drop the database, and then make sure the database gets recreated
            a = myThread.dbi.engine.url.database
            self.assertEqual(myThread.dbi.engine.name, "oracle")
            self.assertIsNone(myThread.dbi.engine.url.database)
            self.assertEqual(myThread.dbi.engine.url.get_backend_name(), "oracle")
            self.assertEqual(myThread.dbi.engine.url.get_driver_name(), "cx_oracle")
            self.assertEqual(myThread.dbi.engine.url.host, "INT2R_NOLB")

            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertTrue(dbName)
            myThread.transaction.processData("SELECT * FROM wmbs_job")

            init.clearDatabase()
            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertTrue(dbName)

            myThread.dbi.processData(destroyDb)

            init.setSchema(modules=['WMCore.WMBS'])
            myThread.transaction.begin()
            myThread.transaction.processData("SELECT * FROM wmbs_job")
            init.clearDatabase()
            dbName = myThread.dbi.processData(selectDbName)[0].fetchall()[0][0]
            self.assertTrue(dbName)
            myThread.transaction.begin()
            init.setSchema(modules=['WMCore.WMBS'])
            myThread.transaction.commit()
        except:
            init.clearDatabase()
            raise
        else:
            init.clearDatabase()



if __name__ == '__main__':
    unittest.main()
