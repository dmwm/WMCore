#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the error handler.

"""

__revision__ = "$Id: Create.py,v 1.7 2009/12/16 18:09:05 delgadop Exp $"
__version__ = "$Revision: 1.7 $"
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



###########################################
####            CORE TABLES            ####
####          Tasks and pilots         ####
####    (defines system operations)    ####
###########################################


        self.create['tqa'] = """      
SET AUTOCOMMIT = 0; """
# Lists tasks in the queue
#    `id` int unsigned NOT NULL AUTO_INCREMENT,
        self.create['10tq_tasks'] = """      
CREATE TABLE `tq_tasks` (
    `id` varchar(255) NOT NULL,
    `spec` varchar(511),
    `sandbox` varchar(511),
    `wkflow` varchar(255),
    `type` varchar(255),
    `reqs` varchar(16383)
    `req_se` varchar(1023),
    `pilot` int unsigned,
    `state` tinyint unsigned NOT NULL default 0,
    `creat_time` timestamp default CURRENT_TIMESTAMP,
    `current_state_time` timestamp default 0,
    PRIMARY KEY `id` (`id`),
    CONSTRAINT `fk_pilot` FOREIGN KEY (`pilot`) REFERENCES `tq_pilots`(`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# TODO: Might have a site field as a reference to a new tq_sites table?
# (if we need to keep more info about sites...)
#    `site` varchar(255),
        self.create['05tq_pilots'] = """      
CREATE TABLE `tq_pilots` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `host` varchar(255),
    `se` varchar(255),
    `cachedir` varchar(511),
    `ttl` int unsigned,
    `ttl_time` timestamp default 0,
    `last_heartbeat` timestamp default CURRENT_TIMESTAMP,
    PRIMARY KEY `id` (`id`)
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



###############################################
####      DATA CACHE-RELATED TABLES        ####
####           Hosts and files             ####
####   (required for data dependencies)    ####
###############################################


# Links hosts and the data stored in their cache
# The field 'se' acts as a site contrain (there might be hosts
# with same name at different sites, but not within the same one)
        self.create['50tq_hostdata'] = """
CREATE TABLE `tq_hostdata` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `host` varchar(255) NOT NULL,
    `se` varchar(255) NOT NULL,
    `data` varchar(255) NOT NULL,
    PRIMARY KEY `id` (`id`),
    CONSTRAINT `fk_data` FOREIGN KEY (`data`) REFERENCES `tq_data`(`guid`),
    CONSTRAINT `uniq_hostdata` UNIQUE (`host`, `se`, `data`) 
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# TODO: This will go away when we move to cache per host
# Links pilots and the data stored in their cache
        self.create['50tq_pilotdata'] = """
CREATE TABLE `tq_pilotdata` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `pilot` int unsigned NOT NULL,
    `data` varchar(255) NOT NULL,
    PRIMARY KEY `id` (`id`),
    CONSTRAINT `fk_pd_data` FOREIGN KEY (`data`) REFERENCES `tq_data`(`guid`),
    CONSTRAINT `fk_pd_pilot` FOREIGN KEY (`pilot`) REFERENCES `tq_pilots`(`id`),
    CONSTRAINT `uniq_pilotdata` UNIQUE (`pilot`, `data`) 
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Lists existing pieces of data (type, size, etc.)
        self.create['15tq_data'] = """      
CREATE TABLE `tq_data` (
    `guid` varchar(255) NOT NULL,
    `type` tinyint unsigned NOT NULL default 0,
    `name` varchar(255),
    `size` int unsigned,
    PRIMARY KEY `guid` (`guid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""



###################################################
####      TABLES THAT NEED TO BE CLEANED       ####
####            Logs and archives              ####
####  (not deleted by normal operation cycle)  ####
###################################################

# Logs events from a pilot (we may not have a log report for it)
        self.create['30tq_pilot_log'] = """      
CREATE TABLE `tq_pilot_log` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `pilot_id` int unsigned NOT NULL,
    `task_id` varchar(255),
    `event` varchar(255),
    `error_code` tinyint unsigned,
    `info` varchar(511),
    `insert_time` timestamp default CURRENT_TIMESTAMP,
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Tasks archival for post-mortem inspection
        self.create['30tq_tasks_archive'] = """      
CREATE TABLE `tq_tasks_archive` (
    `id` varchar(255) NOT NULL,
    `spec` varchar(511),
    `sandbox` varchar(511),
    `wkflow` varchar(255),
    `type` varchar(255),
    `reqs` varchar(16383),
    `req_se` varchar(1023),
    `pilot` int unsigned,
    `state` tinyint unsigned NOT NULL default 0,
    `creat_time` timestamp default CURRENT_TIMESTAMP,
    `current_state_time` timestamp default 0,
    INDEX `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

# Pilots archival for post-mortem inspection
        self.create['30tq_pilots_archive'] = """      
CREATE TABLE `tq_pilots_archive` (
    `id` int unsigned NOT NULL,
    `host` varchar(255),
    `se` varchar(255),
    `cachedir` varchar(511),
    `ttl` int unsigned,
    `ttl_time` timestamp default 0,
    `last_heartbeat` timestamp default CURRENT_TIMESTAMP,
    INDEX `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

