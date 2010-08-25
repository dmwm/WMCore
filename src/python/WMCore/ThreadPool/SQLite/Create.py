#!/usr/bin/python

"""
_Create_

Class for creating SQLite specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.1 2009/05/14 15:17:20 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        # Commented out because this never seems to work in SQLite
#        self.create['a_transaction'] = """
#SET AUTOCOMMIT = 0; """      
        self.create['threadpool'] = """      
CREATE TABLE tp_threadpool(
   id                      int(11)      PRIMARY KEY     NOT NULL,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                   varchard(20) NOT NULL default 'queued',

   FOREIGN KEY(state) references tp_queued_process_enum(value)
   )
"""
        self.create['threadpool_buffer_in'] = """      
CREATE TABLE tp_threadpool_buffer_in(
   id                      int(11)      PRIMARY KEY     NOT NULL,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                   varchard(20) NOT NULL default 'queued',

   FOREIGN KEY(state) references tp_queued_process_enum(value)
   )
"""
        self.create['threadpool_buffer_out'] = """      
CREATE TABLE tp_threadpool_buffer_out(
   id                      int(11)      PRIMARY KEY      NOT NULL,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                   varchard(20) NOT NULL default 'queued',

   FOREIGN KEY(state) references tp_queued_process_enum(value)
   )
"""

        self.create['tp_threadpool_enum'] = """
CREATE TABLE tp_queued_process_enum (
        value varchar(20)       PRIMARY KEY  NOT NULL
        )"""

        self.create['tp_threadpool_enum_insert1'] = """
INSERT INTO tp_queued_process_enum VALUES('queued')
"""
        
        self.create['tp_threadpool_enum_insert2'] = """
INSERT INTO tp_queued_process_enum VALUES('process')
"""

        self.create['tp_threadpool_fktrigger'] = """
CREATE TRIGGER tp_threadpool_fktrigger BEFORE INSERT ON tp_threadpool
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""


        self.create['tp_threadpool_buffer_in_fktrigger'] = """
CREATE TRIGGER tp_threadpool_buffer_in_fktrigger BEFORE INSERT ON tp_threadpool_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool_buffer_in has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""

        self.create['tp_threadpool_buffer_out_fktrigger'] = """
CREATE TRIGGER tp_threadpool_buffer_out_fktrigger BEFORE INSERT ON tp_threadpool_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool_buffer_out has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""

        self.create['tp_threadpool_fktriggeru'] = """
CREATE TRIGGER tp_threadpool_fktriggeru BEFORE UPDATE ON tp_threadpool
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""


        self.create['tp_threadpool_buffer_in_fktriggeru'] = """
CREATE TRIGGER tp_threadpool_buffer_in_fktriggeru BEFORE UPDATE ON tp_threadpool_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool_buffer_in has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""

        self.create['tp_threadpool_buffer_out_fktriggeru'] = """
CREATE TRIGGER tp_threadpool_buffer_out_fktriggeru BEFORE UPDATE ON tp_threadpool_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table tp_threadpool_buffer_out has invalid state')
                WHERE (SELECT procid FROM tp_threadpool_enum WHERE value = NEW.state) IS NULL;
             END;"""
