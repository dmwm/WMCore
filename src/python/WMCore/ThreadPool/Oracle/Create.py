#!/usr/bin/python

"""
_Create_

Class for creating Oracle specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.2 2009/05/15 15:52:22 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating Oracle specific schema for persistent messages.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
#Disabled for now: Oracle doesn't seem to support SET AUTOCOMMIT on non-interactive runs.
#        self.create['a_transaction'] = """
#SET AUTOCOMMIT OFF; """

        #tp_threadpool

        self.create['threadpool'] = """      
CREATE TABLE tp_threadpool(
   id                      NUMBER(11)    NOT NULL ENABLE,
   event                   varchar2(255) NOT NULL ENABLE,
   component               varchar2(255) NOT NULL ENABLE,
   payload                 clob          NOT NULL ENABLE,
   thread_pool_id          varchar2(255) NOT NULL ENABLE,
   state                   varchar2(20)  default 'queued'  NOT NULL ENABLE,
   CONSTRAINT tp_threadpool_state CHECK(state IN ('queued', 'process')),
   CONSTRAINT tp_threadpool_pk    PRIMARY KEY (id)
   )
"""
        #Because Oracle does not implement AUTOINCREMENT, we have to do it ourselves.
        #Create a sequence of integers and a trigger to do it ourselves.
        
        self.create['threadpool_seq'] = """
CREATE SEQUENCE tp_threadpool_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['threadpool_trg'] = """
CREATE TRIGGER tp_threadpool_trg
BEFORE INSERT ON tp_threadpool
FOR EACH ROW
     DECLARE m_no INTEGER;
     BEGIN
        SELECT tp_threadpool_seq.nextval INTO :new.id FROM dual;
     END;        """



        #tp_threadpool_buffer_in

        self.create['threadpool_buffer_in'] = """      
CREATE TABLE tp_threadpool_buffer_in(
   id                      NUMBER(11)      NOT NULL ENABLE,
   event                   varchar(255)    NOT NULL ENABLE,
   component               varchar(255)    NOT NULL ENABLE,
   payload                 clob            NOT NULL ENABLE,
   thread_pool_id          varchar(255)    NOT NULL ENABLE,
   state                   varchar2(20)    default 'queued'  NOT NULL ENABLE,
   CONSTRAINT tp_threadpool_buffer_in_state CHECK(state IN ('queued', 'process')),
   CONSTRAINT tp_threadpool_buffer_in_pk    PRIMARY KEY (id)
   )
"""
        self.create['threadpool_buffer_in_seq'] = """
CREATE SEQUENCE tp_buffer_in_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['threadpool_buffer_in_trg'] = """
CREATE TRIGGER tp_buffer_in_trg
BEFORE INSERT ON tp_threadpool_buffer_in
FOR EACH ROW
     BEGIN
        SELECT tp_buffer_in_seq.nextval INTO :new.id FROM dual;
     END;        """


        #tp_threadpool_buffer_out
        
        self.create['threadpool_buffer_out'] = """      
CREATE TABLE tp_threadpool_buffer_out(
   id                      NUMBER(11)   NOT NULL ENABLE,
   event                   varchar(255) NOT NULL ENABLE,
   component               varchar(255) NOT NULL ENABLE,
   payload                 clob         NOT NULL ENABLE,
   thread_pool_id          varchar(255) NOT NULL ENABLE,
   state                   varchar2(20) default 'queued'  NOT NULL ENABLE,
   CONSTRAINT tp_threadpool_buffer_out_state CHECK(state IN ('queued', 'process')),
   CONSTRAINT tp_threadpool_buffer_out_pk    PRIMARY KEY (id)
   )
"""


        self.create['threadpool_buffer_out_seq'] = """
CREATE SEQUENCE tp_buffer_out_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['threadpool_buffer_out_trg'] = """
CREATE TRIGGER tp_threadpool_buffer_out_trg
BEFORE INSERT ON tp_threadpool_buffer_out
FOR EACH ROW
     BEGIN
        SELECT tp_buffer_out_seq.nextval INTO :new.id FROM dual;
     END;        """
 
