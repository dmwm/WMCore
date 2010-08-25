#!/usr/bin/env python

"""
_Queries_

This module implements the Oracle backend for the persistent threadpool.

"""

__revision__ = \
    "$Id: Queries.py,v 1.1 2009/05/14 16:50:31 mnorman Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "mnorman@fnal.gov"

import threading

from WMCore.Database.DBFormatter import DBFormatter

from WMCore.Threadpool.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_
    
    This module implements the Oracle backend for the persistent threadpool.
    
    """
    
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





