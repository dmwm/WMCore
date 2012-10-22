#!/usr/bin/env python

"""
_Queries_

This module implements the Oracle backend for the persistent threadpool.

"""



import threading

from WMCore.Database.DBFormatter import DBFormatter

from WMCore.ThreadPool.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_

    This module implements the Oracle backend for the persistent threadpool.

    """


    def selectWork(self, args, pooltable = 'tp_threadpool'):
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



    def updateWorkStatus(self, args, pooltable = 'tp_threadpool'):
        """
        Updates work status of work being processed.
        """

        # differentiate between onequeu and multi queue
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
            'tp_threadpool_buffer_out']:
            sqlStr = """
UPDATE %s SET state='queued' WHERE component = :componentName
AND thread_pool_id = :thread_pool_id
        """ % (pooltable)
            self.execute(sqlStr, args)
        else:
            sqlStr = """
UPDATE %s SET state="queued"
        """ % (pooltable)
            self.execute(sqlStr, {})

        return


    def moveWorkFromBufferIn(self, source, target):
        """
        Moves work from buffer in to main queue or buffer out
        """
        sqlUpd1 = ''
        sqlStr1 = ''
        sqlStr2 = ''
        if source == 'tp_threadpool_buffer_in':
            sqlUpd1 = """
CURSOR %s_c1 IS
  SELECT event, component, payload, thread_pool_id FROM %s
  FOR UPDATE
            """ %(target, source)
            sqlStr1 = """
INSERT INTO %s(event,component,payload,thread_pool_id) SELECT event,component,payload,thread_pool_id FROM %s
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)
            #self.execute(sqlUpd1, {})
        else:
            sqlStr1 = """
INSERT INTO %s(event,payload) SELECT event,payload FROM %s
            """ % (target, source)
            sqlStr2 = """ DELETE FROM %s """ % (source)

        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})

        return

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
INSERT INTO %s (event, component, payload, thread_pool_id)
  SELECT event, component, payload, thread_pool_id FROM
   (SELECT event, component, payload, thread_pool_id, ROWNUM rn FROM %s
   WHERE component=:component AND thread_pool_id=:thread_pool_id)
  WHERE rn < %s
   """ % (target, source, limit)
            sqlStr2 = """
DELETE FROM %s WHERE id IN
(SELECT id FROM
(SELECT id, ROWNUM rn FROM %s
 WHERE component = :component
 AND thread_pool_id = :thread_pool_id
)
 WHERE rn < %s
)
""" % (source, source, limit)
            self.execute(sqlStr1, args)
            self.execute(sqlStr2, args)
        else:
            sqlStr1 = """
INSERT INTO %s(event,payload) VALUES
 (SELECT event, payload FROM
  (SELECT event, payload, ROWNUM rn FROM %s)
 WHERE rn < %s
 )
            """ % (target, source, limit)
            sqlStr2 = """
DELETE FROM %s WHERE id IN
 (SELECT id FROM
  (
   SELECT id, ROWNUM rn FROM %s
   ORDER BY id
  )
  WHERE rn < %s
 )
            """ % (source, source, limit )
            self.execute(sqlStr1, {})
            self.execute(sqlStr2, {})

        return



    def insertThreadPoolTables(self, threadpool):
        """
        __insertThreadPoolTable

        Inserts tables for threadpool (one for each threadpool)
        when multi queue is enabled.
        """
        sqlStr = """
CREATE TABLE %s(
   id                      NUMBER(11)    NOT NULL ENABLE,
   event                   varchar2(255) NOT NULL ENABLE,
   payload                 long          NOT NULL ENABLE,
   state                   varchar2(20)  NOT NULL ENABLE,

   CONSTRAINT  %s_pk     PRIMARY KEY (id),
   CONSTRAINT  %s_state  CHECK(state IN ('queued', 'process'))
   ); """ % (threadpool, threadpool, threadpool)

        self.execute(sqlStr, {})


        seqStr = """CREATE SEQUENCE %s_seq
        start with 1
        increment by 1
        nomaxvalue
        """ %(threadpool)

        self.execute(seqStr, {})

        trgStr = """CREATE TRIGGER %s_trg
BEFORE INSERT ON %s
FOR EACH ROW
     BEGIN
        SELECT %s_seq.nextval INTO :new.id FROM dual;
     END;
        """ %(threadpool)

        self.execute(trgStr, {})

        return


    def insertWork(self, args, pooltable = 'tp_threadpool'):
        """
        Inserts work into the database in case no thread can be found.
        """

        # differentiate between onequeu and multi queue
        if pooltable in ['tp_threadpool', 'tp_threadpool_buffer_in', \
            'tp_threadpool_buffer_out']:
            sqlStr = """
INSERT INTO %s(event,component,payload,thread_pool_id)
VALUES(:event,:component,:payload,:thread_pool_id)
        """  % (pooltable)
            self.execute(sqlStr, args)
        else:
            sqlStr = """
INSERT INTO %s(event,payload)
VALUES(:event,:payload)
        """  % (pooltable)
            self.execute(sqlStr, {'event':args['event'], \
                'payload':args['payload']})

        return
