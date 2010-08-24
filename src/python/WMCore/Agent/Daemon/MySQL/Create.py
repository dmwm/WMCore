#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for 
keeping track of daemonized processes
"""





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
        self.create['taa'] = """      
SET AUTOCOMMIT = 0; """
        self.create['ta_daemon_reg'] = """      
CREATE TABLE `daemon_registry` (
    `componentName`     varchar(100) NOT NULL,
    `processid`        int(11) NOT NULL,
    `parentProcessId`  int(11) NOT NULL,
    `processGroupId`   int(11) NOT NULL,
    `userId`           int(11) NOT NULL,
    `effectiveUserId`  int(11) NOT NULL,
    `groupId`          int(11) NOT NULL,
    `effectiveGroupId` int(11) NOT NULL,
    PRIMARY KEY `processid` (`processid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
 
