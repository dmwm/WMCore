#!/usr/bin/python

"""
_Create_

Class for creating MySQL specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.2 2008/08/18 15:00:13 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"


from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """
    
    
    
    def __init__(self, logger, dbinterface):
        DBCreator.__init__(self, logger, dbinterface)
        self.create = {}
        self.constraints = {}
        msg = """
A ms_type table stores information on message types.

Fields:

 typeid   id
 name     message name
        """
        logger.debug(msg)
        self.create['ta_ms_type'] = """      
CREATE TABLE `ms_type` (
    `typeid` int(11) NOT NULL auto_increment,
    `name` varchar(255) NOT NULL default '',
    PRIMARY KEY `typeid` (`typeid`),
    UNIQUE (`name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        msg =  """ 
A ms_process table stores information on components.
                                                                                
Fields:
                                                                                
procid   id
name     component name
host     host name
pid      process id in host name 
"""
        logger.debug(msg)
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
        
        msg = """
A ms_history table stores information on the complete message history.
                                                                                
Fields:
                                                                                
 messageid   id
 type        message type id
 source      source component id
 dest        target component id
 payload     message payload
 time        time stamp
"""
        logger.debug(msg)
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
        self.create['td_ms_priority_history'] = """
CREATE TABLE `ms_priority_history` (
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
        
        msg = """
A ms_message table stores information on the messages to be delivered.
                                                                                
Fields:
                                                                                
 messageid   id
 type        message type id
 source      source component id
 dest        target component id
 payload     message payload
 time        time stamp
"""
        logger.debug(msg)
        self.create['te_ms_message'] = """
CREATE TABLE `ms_message` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        msg = """
ms_message_buffer_in: an input buffer for the message queue
to prevent inserting messages one, by one in the message queu.
"""
        logger.debug(msg)
        self.create['tf_ms_message_buffer_in'] = """
CREATE TABLE `ms_message_buffer_in` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
   """
       
        msg = """
ms_message_buffer_out: an output buffer for the message queue
to prevent removing message one by one, out of a potential large queue.
"""
        logger.debug(msg) 
        self.create['tg_ms_message_buffer_out'] = """ 
CREATE TABLE `ms_message_buffer_out` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
)  ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        msg = """
ms_priority_message: a table for priority messages.
The message service will first examine this table before
looking at the other messages.
"""
        logger.debug(msg)
        self.create['th_ms_priorty_message'] = """
CREATE TABLE `ms_priority_message` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['ti_ms_priority_message_buffer_in'] = """
CREATE TABLE `ms_priority_message_buffer_in` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['tj_ms_priority_message_buffer_out'] = """
CREATE TABLE `ms_priority_message_buffer_out` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished'),

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        msg = """

A ms_subscription table stores information on the message subscriptions.
                                                                                
Fields:

 subid   id
 procid  component id
 typeid  message type id
"""
        logger.debug(msg)
        self.create['tk_ms_subscription'] = """
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
        self.create['tl_ms_priority_subscription'] = """
CREATE TABLE `ms_priority_subscription` (
   `subid` int(11) NOT NULL auto_increment,
   `procid` int(11) NOT NULL default '0',
   `typeid` int(11) NOT NULL default '0',
   KEY `subid` (`subid`),
   UNIQUE (`procid`,`typeid`),
   FOREIGN KEY(`procid`) references `ms_process`(`procid`),
   FOREIGN KEY(`typeid`) references `ms_type`(`typeid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        
            
    
            