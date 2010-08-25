#!/usr/bin/env python
"""
_Queries_

This module implements the SQLite backend for the persistent threadpool.
"""

__revision__ = "$Id: Queries.py,v 1.3 2009/08/13 00:09:09 meloam Exp $"
__version__ = "$Revision: 1.3 $"

import threading

from WMCore.ThreadPool.MySQL.Queries import Queries as MySQLQueries

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
   id                      INTEGER PRIMARY KEY AUTOINCREMENT,
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

        self.execute(sqlStr, {})
        self.execute(sqlString1, {})
        self.execute(sqlString2, {})
        self.execute(sqlString3, {})
        self.execute(sqlTriggerString, {})

        return

    def moveWorkToBufferOut(self, args, source, target, limit):
        """
        _moveWorkToBufferOut_

        Moves work from buffer in or main queueu to the buffer out table.

        Note:  This differs from the MySQL code in that the SELECT statements
        do not have "FOR UPDATE" clauses.  The DELETE statements have also been
        changed as my version of SQLite doesn't support the ORDER BY clause.
        """
        if source in ["tp_threadpool", "tp_threadpool_buffer_in",
                      "tp_threadpool_buffer_out"]:
            sqlStr1 = """INSERT INTO %s (event, component, payload, thread_pool_id) 
                           SELECT event, component, payload, thread_pool_id FROM %s 
                           WHERE component= :component AND thread_pool_id = :thread_pool_id 
                           ORDER BY id LIMIT %s""" % (target, source, limit)
            sqlStr2 = """DELETE FROM %s 
                         WHERE component= :component AND thread_pool_id = :thread_pool_id
                         AND id IN (SELECT id FROM %s ORDER BY id LIMIT %s)
                         """ % (source, source, limit)

            self.execute(sqlStr1, args)
            self.execute(sqlStr2, args)
        else:
            sqlStr1 = """INSERT INTO %s (event, payload)
                           SELECT event, payload FROM %s ORDER BY id LIMIT %s""" % (target, source, limit)
            sqlStr2 = """DELETE FROM %s WHERE id IN
                           (SELECT id FROM %s ORDER BY id LIMIT %s)""" % (source, source, limit)
            
            self.execute(sqlStr1, {})
            self.execute(sqlStr2, {})

        return
    
    def moveWorkFromBufferIn(self, source, target):
        """
        Moves work from buffer in to main queue or buffer out
        """

        sqlStr1 = ''
        sqlStr2 = ''
        if source == 'tp_threadpool_buffer_in':
            sqlStr1 = """
INSERT INTO %s(event,component,payload,thread_pool_id) SELECT event,component,payload,thread_pool_id FROM %s
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)
        else:
            sqlStr1 = """
INSERT INTO %s(event,payload) SELECT event,payload FROM %s
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)
        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})
