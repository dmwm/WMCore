#!/usr/bin/python

"""
_Create_

Class for creating MySQL specific schema for the WorkflowManager

"""

__revision__ = "$Id: Create.py,v 1.1 2009/02/04 21:57:11 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "james.jackson@cern.ch"

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
        self.create['ta_managed_workflow_mapping'] = """
CREATE TABLE `managed_workflow` (
    id               INTEGER         PRIMARY KEY AUTO_INCREMENT,
    fileset_match    VARCHAR(256)    NOT NULL,
    workflow         INT(11)         NOT NULL,
    split_algo       VARCHAR(256)    NOT NULL,
    type             VARCHAR(256)    NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

        self.create['ta_managed_workflow_mapping_location'] = """
CREATE TABLE managed_workflow_location (
    managed_workflow    INT(11)      NOT NULL,
    location            INT(11)      NOT NULL,
    valid               BOOLEAN      NOT NULL DEFAULT TRUE,
    FOREIGN KEY(managed_workflow_mapping)  REFERENCES managed_workflow_mapping(id)
        ON DELETE CASCADE,
    FOREIGN KEY(location)     REFERENCES wmbs_location(id)
        ON DELETE CASCADE)
"""
