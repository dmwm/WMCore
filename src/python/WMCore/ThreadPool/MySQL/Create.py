#!/usr/bin/python

"""
_Create_

Class for creating MySQL specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.1 2008/09/04 12:30:17 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging
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
        self.create['a_transaction'] = """
SET AUTOCOMMIT = 0; """      
        self.create['threadpool'] = """      
CREATE TABLE tp_threadpool(
   id                      int(11)      NOT NULL auto_increment,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                 enum('queued','process') default 'queued',
   primary key(id)
   ) TYPE=InnoDB;
"""
        self.create['threadpool_buffer_in'] = """      
CREATE TABLE tp_threadpool_buffer_in(
   id                      int(11)      NOT NULL auto_increment,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                 enum('queued','process') default 'queued',
   primary key(id)
   ) TYPE=InnoDB;
"""
        self.create['threadpool_buffer_out'] = """      
CREATE TABLE tp_threadpool_buffer_out(
   id                      int(11)      NOT NULL auto_increment,
   event                   varchar(255) NOT NULL,
   component               varchar(255) NOT NULL,
   payload                 text         NOT NULL,
   thread_pool_id          varchar(255) NOT NULL,
   state                 enum('queued','process') default 'queued',
   primary key(id)
   ) TYPE=InnoDB;
"""
 
