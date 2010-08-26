#!/usr/bin/env python
"""
_Create_

Class for creating SQLite specific schema for persistent messages.
"""

__revision__ = "$Id: Create.py,v 1.2 2009/07/17 16:01:32 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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

        self.create['threadpool'] = """      
CREATE TABLE tp_threadpool(
   id                      integer      PRIMARY KEY AUTOINCREMENT,
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
   id                      integer      PRIMARY KEY AUTOINCREMENT,
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
   id                      integer      PRIMARY KEY AUTOINCREMENT,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                   varchard(20) NOT NULL default 'queued',

   FOREIGN KEY(state) references tp_queued_process_enum(value)
   )
"""

        self.create['tp_queued_process_enum'] = """
CREATE TABLE tp_queued_process_enum (
        value varchar(20)       PRIMARY KEY  NOT NULL
        )"""

        self.create['tp_queued_process_enum_insert1'] = """
INSERT INTO tp_queued_process_enum VALUES('queued')
"""
        
        self.create['tp_queued_process_enum_insert2'] = """
INSERT INTO tp_queued_process_enum VALUES('process')
"""
