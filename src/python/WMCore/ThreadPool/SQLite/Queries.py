#!/usr/bin/env python

"""
_Queries_

This module implements the SQLite backend for the persistent threadpool.

"""

__revision__ = "$Id: Queries.py,v 1.1 2009/05/14 15:18:32 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.Threadpool.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_

    This module implements the SQLite backend for the persistent threadpool.

    """

    def insertThreadPoolTables(self, threadpool):
        """
        __insertThreadPoolTable

        Inserts tables for threadpool (one for each threadpool)
        when multi queue is enabled. SQLite version
        """

        #The usual SQLite magic has to happen here
        # - mnorman
        
        sqlStr = """
CREATE TABLE %s(
   id                      INTEGER PRIMARY KEY,
   event                   varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   state                 enum('queued','process') default 'queued',
   )  """ % (threadpool)

        sqlString1 = """
CREATE TABLE %s_enum (
        value varchar(20)       PRIMARY KEY  NOT NULL
        )
        """ %(threadpool)

        sqlString2 = """
INSERT INTO %s_enum VALUES('queued')
        """ %(threadpool)

        sqlString3 = """
INSERT INTO %s_enum VALUES('process')
        """ %(threadpool)

        sqlTriggerString = """
CREATE TRIGGER %s_trigger BEFORE INSERT ON %s
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table %s has invalid state')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;
        """ %(threadpool)

        self.execute(sqlStr, {})
        self.execute(sqlString1, {})
        self.execute(sqlString2, {})
        self.execute(sqlString3, {})
        self.execute(sqlTriggerString, {})


        return


