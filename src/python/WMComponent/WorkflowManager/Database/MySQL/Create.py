#!/usr/bin/env python
#pylint: disable-msg=E1103
"""
_Create_

Class for creating MySQL specific schema for the WorkflowManager

"""

__revision__ = "$Id: Create.py,v 1.4 2009/10/07 09:50:06 spiga Exp $"
__version__ = "$Revision: 1.4 $"
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
        self.create['ta_wm_managed_workflow'] = """
CREATE TABLE `wm_managed_workflow` (
    id               INTEGER         PRIMARY KEY AUTO_INCREMENT,
    workflow         INT(11)         NOT NULL,
    fileset_match    VARCHAR(256)    NOT NULL,
    split_algo       VARCHAR(256)    NOT NULL,
    type             VARCHAR(256)    NOT NULL,
    UNIQUE (workflow, fileset_match),
    FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
        ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

        self.create['ta_wm_managed_workflow_location'] = """
CREATE TABLE wm_managed_workflow_location (
    managed_workflow    INT(11)      NOT NULL,
    location            INT(11)      NOT NULL,
    valid               BOOLEAN      NOT NULL DEFAULT TRUE,
    UNIQUE (managed_workflow, location),
    FOREIGN KEY(managed_workflow) REFERENCES wm_managed_workflow(id)
        ON DELETE CASCADE,
    FOREIGN KEY(location) REFERENCES wmbs_location(id)
        ON DELETE CASCADE)
"""
