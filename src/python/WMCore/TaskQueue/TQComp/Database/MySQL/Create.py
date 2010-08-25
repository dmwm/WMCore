#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the error handler.

"""

__revision__ = "$Id: Create.py,v 1.1 2009/04/27 08:21:20 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "delgadop@cern.ch"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for task list.
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['tqa'] = """      
SET AUTOCOMMIT = 0; """
        self.create['tq_tasks'] = """      
CREATE TABLE `tq_tasks` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `spec` varchar(255),
    `sandbox` varchar(255),
    `wkflow` varchar(255),
    `type` int(11) NOT NULL default 0,
    `pilot` int(11),
    `state` int(11) NOT NULL default 0,
    `creat_time` timestamp default CURRENT_TIMESTAMP,
    `current_state_time` timestamp,
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['tq_pilots'] = """      
CREATE TABLE `tq_pilots` (
    `id` int(11) NOT NULL,
    `host` varchar(255),
    `data` int(11),
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
 
        self.create['tq_data'] = """      
CREATE TABLE `tq_data` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `type` int(11) NOT NULL default 0,
    `name` varchar(255),
    `size` int(11),
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
