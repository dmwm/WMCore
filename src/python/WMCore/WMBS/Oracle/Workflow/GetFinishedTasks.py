#!/usr/bin/env python
"""
_GetFinishedTasks_

Oracle implementation of Workflow.GetFinishedTasks

Created on Nov 7, 2012

@author: dballest
"""

from WMCore.WMBS.MySQL.Workflow.GetFinishedTasks import GetFinishedTasks as MySQLGetFinishedTasks

class GetFinishedTasks(MySQLGetFinishedTasks):
    pass
