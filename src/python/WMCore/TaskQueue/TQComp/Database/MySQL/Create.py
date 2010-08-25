#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the error handler.

"""

__revision__ = "$Id: Create.py,v 1.3 2009/07/08 17:28:07 delgadop Exp $"
__version__ = "$Revision: 1.3 $"
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
# Lists tasks in the queue
#    `id` int(11) NOT NULL AUTO_INCREMENT,
        self.create['tq_tasks'] = """      
CREATE TABLE `tq_tasks` (
    `id` varchar(255) NOT NULL,
    `spec` varchar(255),
    `sandbox` varchar(255),
    `wkflow` varchar(255),
    `type` varchar(255),
    `reqs` varchar(510),
    `req_se` varchar(510),
    `pilot` int(11),
    `state` int(2) NOT NULL default 0,
    `creat_time` timestamp default CURRENT_TIMESTAMP,
    `current_state_time` timestamp default 0,
    PRIMARY KEY `id` (`id`),
    CONSTRAINT `fk_pilot` FOREIGN KEY (`pilot`) REFERENCES `tq_pilots`(`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Won't use a tq_req_se, rather comma separated ses for tq_tasks.req_se
## Relates taskIds with required site to run
#        self.create['tq_req_se'] = """      
#CREATE TABLE `tq_req_se` (
#    `task` varchar(255) NOT NULL,
#    `se` varchar(255)
#    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
#"""

# TODO: Might have a site field as a reference to a new tq_sites table
# (if we need to keep more info about sites...)
# Need to look at ResoureMonitor for pilot manager first to decide what to do
#    `site` varchar(255),
        self.create['tq_pilots'] = """      
CREATE TABLE `tq_pilots` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `host` varchar(255),
    `se` varchar(255),
    `cachedir` varchar(255),
    `ttl` int(11),
    `ttl_time` timestamp default 0,
    `last_heartbeat` timestamp default CURRENT_TIMESTAMP,
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Links hosts and the data stored in their cache
# The field 'se' acts as a site contrain (there might be hosts
# with same name at different sites, but not within the same one)
        self.create['tq_hostdata'] = """
CREATE TABLE `tq_hostdata` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `host` varchar(255) NOT NULL,
    `se` varchar(255) NOT NULL,
    `data` varchar(255) NOT NULL,
    PRIMARY KEY `id` (`id`),
    CONSTRAINT `fk_data` FOREIGN KEY (`data`) REFERENCES `tq_data`(`guid`),
    CONSTRAINT `uniq_hostdata` UNIQUE (`host`, `se`, `data`) 
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Lists existing pieces of data (type, size, etc.)
        self.create['tq_data'] = """      
CREATE TABLE `tq_data` (
    `guid` varchar(255) NOT NULL,
    `type` int(11) NOT NULL default 0,
    `name` varchar(255),
    `size` int(11),
    PRIMARY KEY `guid` (`guid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
