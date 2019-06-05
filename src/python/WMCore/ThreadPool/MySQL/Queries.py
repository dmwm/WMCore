#!/usr/bin/env python

"""
_Queries_

This module implements the mysql backend for the persistent threadpool.

"""

import threading

from WMCore.Database.DBFormatter import DBFormatter


class Queries(DBFormatter):
    """
    _Queries_

    This module implements the mysql backend for the persistent threadpool.

    """

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def selectWork(self, args, pooltable='tp_threadpool'):
        """
        Select work that is not yet being processed.
        """
        # this query takes place in a locked section so
        # we do not have to worry about multiple slaves
        # getting the same work.
        result = ''
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
                         'tp_threadpool_buffer_out']:
            sqlStr = """
SELECT min(id) FROM %s WHERE component = :component AND
thread_pool_id = :thread_pool_id AND state='queued'
        """ % (pooltable)
            result = self.execute(sqlStr, args)
        else:
            sqlStr = """
SELECT min(id) FROM %s WHERE state='queued'
        """ % (pooltable)
            result = self.execute(sqlStr, {})
        return self.formatOne(result)

    def retrieveWork(self, args, pooltable='tp_threadpool'):
        """
        Retrieve work (its payload) from a queue.
        """

        sqlStr = """
SELECT id, event,payload FROM %s WHERE id = :id
        """ % (pooltable)
        result = self.execute(sqlStr, args)
        return self.formatOne(result)

    def tagWork(self, args, pooltable='tp_threadpool'):
        """
        Tag work in queue as being processed.
        """
        sqlStr = """
UPDATE %s SET state='process' WHERE id = :id
        """ % (pooltable)
        self.execute(sqlStr, args)

    def removeWork(self, args, pooltable='tp_threadpool'):
        """
        Remove work from the queue.
        """
        sqlStr = """
DELETE FROM %s WHERE id = :id
        """ % (pooltable)
        self.execute(sqlStr, args)

    def updateWorkStatus(self, args, pooltable='tp_threadpool'):
        """
        Updates work status of work being processed.
        """
        # differentiate between one queue and multi queue
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
                         'tp_threadpool_buffer_out']:
            sqlStr = """UPDATE %s SET state = 'queued'
                          WHERE component = :componentName AND
                                thread_pool_id = :thread_pool_id
                     """ % (pooltable)
            self.execute(sqlStr, args)
        else:
            sqlStr = "UPDATE %s SET state = 'queued'" % (pooltable)
            self.execute(sqlStr, {})

        return

    def getQueueLength(self, args, pooltable='tp_threadpool'):
        """
        Returns the queue length from the different tables
        through a parameterized query.
        """
        # differentiate between onequeu and multi queue
        result = None
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
                         'tp_threadpool_buffer_out']:
            sqlStr = """
SELECT COUNT(*) FROM  %s WHERE component = :componentName
AND thread_pool_id = :thread_pool_id
        """ % (pooltable)
            result = self.execute(sqlStr, args)
        else:
            sqlStr = """
SELECT COUNT(*) FROM  %s
        """ % (pooltable)
            result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]

    def moveWorkFromBufferIn(self, source, target):
        """
        Moves work from buffer in to main queue or buffer out
        """

        sqlStr1 = ''
        sqlStr2 = ''
        if source == 'tp_threadpool_buffer_in':
            sqlStr1 = """
INSERT INTO %s(event,component,payload,thread_pool_id) SELECT event,component,payload,thread_pool_id FROM %s FOR UPDATE
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)
        else:
            sqlStr1 = """
INSERT INTO %s(event,payload) SELECT event,payload FROM %s
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)
        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})

    def moveWorkToBufferOut(self, args, source, target, limit):
        """
        Moves work from buffer in or main queue to the buffer out
        table.
        """

        sqlStr1 = ''
        sqlStr2 = ''
        if source in ['tp_threadpool', 'tp_threadpool_buffer_in', \
                      'tp_threadpool_buffer_out']:
            # we need a for update in the select to prevent (harmless) deadlock
            # situations with innodb
            sqlStr1 = """
INSERT INTO %s(event,component,payload,thread_pool_id)
SELECT event,component,payload,thread_pool_id FROM %s
WHERE component= :component AND thread_pool_id = :thread_pool_id
ORDER BY id LIMIT %s FOR UPDATE
            """ % (target, source, limit)
            sqlStr2 = """
DELETE FROM %s
WHERE component= :component AND thread_pool_id = :thread_pool_id
ORDER BY id LIMIT %s""" % (source, limit)
            self.execute(sqlStr1, args)
            self.execute(sqlStr2, args)
        else:
            sqlStr1 = """
INSERT INTO %s(event,payload) SELECT event,payload FROM %s ORDER BY id LIMIT %s
            """ % (target, source, limit)
            sqlStr2 = """
DELETE FROM %s ORDER BY id LIMIT %s
            """ % (source, limit)
            self.execute(sqlStr1, {})
            self.execute(sqlStr2, {})

    def insertWork(self, args, pooltable='tp_threadpool'):
        """
        Inserts work into the database in case no thread can be found.
        """
        # differentiate between onequeu and multi queue
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
                         'tp_threadpool_buffer_out']:
            sqlStr = """
INSERT INTO %s(event,component,payload,thread_pool_id)
VALUES(:event,:component,:payload,:thread_pool_id)
        """ % (pooltable)
            self.execute(sqlStr, args)
        else:
            sqlStr = """
INSERT INTO %s(event,payload)
VALUES(:event,:payload)
        """ % (pooltable)
            self.execute(sqlStr, {'event': args['event'], \
                                  'payload': args['payload']})

    def insertThreadPoolTables(self, threadpool):
        """
        __insertThreadPoolTable

        Inserts tables for threadpool (one for each threadpool)
        when multi queue is enabled.
        """
        sqlStr = """
CREATE TABLE %s(
   id                      int(11)      NOT NULL auto_increment,
   event                   varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   state                 enum('queued','process') default 'queued',
   primary key(id)
   ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC""" % (threadpool)

        self.execute(sqlStr, {})

    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        # FIXME: we use this method in all kinds of places perhaps upgrade
        # FIXME: this method?
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args)
