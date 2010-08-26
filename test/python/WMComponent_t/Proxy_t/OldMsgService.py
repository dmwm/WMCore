#!/usr/bin/python
#pylint: disable-msg=E1103

"""

Class for creating MySQL specific schema old message service.

"""

__revision__ = "$Id: OldMsgService.py,v 1.2 2008/10/01 11:09:13 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBCreator import DBCreator

class OldMsgService(DBCreator):
    """
    Class for creating MySQL specific schema old message service.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['taa'] = """      
SET AUTOCOMMIT = 0; """
        self.create['ta_ms_type'] = """      
CREATE TABLE `ms_type` (
  `typeid` int(11) NOT NULL auto_increment,
  `name` varchar(255) NOT NULL default '',
  PRIMARY KEY `typeid` (`typeid`),
  UNIQUE (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['tb_ms_process'] = """      
CREATE TABLE `ms_process` (
  `procid` int(11) NOT NULL auto_increment,
  `name` varchar(255) NOT NULL default '',
  `host` varchar(255) NOT NULL default '',
  `pid` int(11) NOT NULL default '0',
  PRIMARY KEY `procid` (`procid`),
  UNIQUE (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['tc_ms_history'] = """      
CREATE TABLE `ms_history` (
  `messageid` int(11) NOT NULL auto_increment,
  `type` int(11) NOT NULL default '0',
  `source` int(11) NOT NULL default '0',
  `dest` int(11) NOT NULL default '0',
  `payload` text NOT NULL,
  `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `delay` varchar(50) NOT NULL default '00:00:00',

  PRIMARY KEY `messageid` (`messageid`),
  FOREIGN KEY(`type`) references `ms_type`(`typeid`),
  FOREIGN KEY(`source`) references `ms_process`(`procid`),
  FOREIGN KEY(`dest`) references `ms_process`(`procid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['td_ms_message'] = """      
CREATE TABLE `ms_message` (
  `messageid` int(11) NOT NULL auto_increment,
  `type` int(11) NOT NULL default '0',
  `source` int(11) NOT NULL default '0',
  `dest` int(11) NOT NULL default '0',
  `payload` text NOT NULL,
  `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `delay` varchar(50) NOT NULL default '00:00:00',

  PRIMARY KEY `messageid` (`messageid`),
  FOREIGN KEY(`type`) references `ms_type`(`typeid`),
  FOREIGN KEY(`source`) references `ms_process`(`procid`),
  FOREIGN KEY(`dest`) references `ms_process`(`procid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['te_ms_subscription'] = """      
CREATE TABLE `ms_subscription` (
  `subid` int(11) NOT NULL auto_increment,
  `procid` int(11) NOT NULL default '0',
  `typeid` int(11) NOT NULL default '0',
  KEY `subid` (`subid`),
  UNIQUE (`procid`,`typeid`),
  FOREIGN KEY(`procid`) references `ms_process`(`procid`),
  FOREIGN KEY(`typeid`) references `ms_type`(`typeid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""



 
