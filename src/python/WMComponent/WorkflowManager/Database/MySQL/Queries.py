#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the 
WorkflowManager

"""

import time

__revision__ = "$Id: Queries.py,v 1.1 2009/02/04 21:57:11 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "james.jackson@cern.ch"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the MySQL backend for the WorkflowManager
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def markLocation(self, managedWorkflow, valid, location):
        """
        Checks if a given feeder type is already instantiated
        """
        sqlStr = """insert into managed_workflow_location 
            (managed_workflow, location, valid)
        select :managed_workflow, id, :valid from wmbs_location 
        where se_name = :location"""
        result = self.execute(sqlStr, {'managed_workflow' : managedWorkflow, \
                                       'valid' : valid, \
                                       'location' : location})
