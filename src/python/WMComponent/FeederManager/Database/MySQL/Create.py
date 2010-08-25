#!/usr/bin/python

"""
_Create_

Class for creating MySQL specific schema for the FeederManager

"""

__revision__ = "$Id: Create.py,v 1.3 2009/10/07 09:49:52 spiga Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "james.jackson@cern.ch"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """
    
    def __init__(self,logger=None, dbi=None, params = None):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['taa'] = """      
SET AUTOCOMMIT = 0; """
        self.create['ta_managed_feeders'] = """
CREATE TABLE `managed_feeders` (
    id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
    feeder_type  VARCHAR(256) NOT NULL,
    feeder_state VARCHAR(256) NOT NULL,
    insert_time  INT(11)      NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
        self.create['ta_managed_filesets'] = """
CREATE TABLE `managed_filesets` (
    fileset      INT(11)      NOT NULL,
    feeder       INT(11)      NOT NULL,
    insert_time  INT(11)      NOT NULL,
    FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
        ON DELETE CASCADE,
    FOREIGN KEY(feeder) REFERENCES managed_feeders(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
