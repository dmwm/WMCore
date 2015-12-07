#!/usr/bin/env python
"""
_GetFinishedWorkflows_

Oracle implementation of Workflow.GetFinishedWorkflows

Created on Aug 30, 2012

@author: dballest
"""

from WMCore.WMBS.MySQL.Workflow.GetFinishedWorkflows import GetFinishedWorkflows as MySQLGetFinishedWorkflows

class GetFinishedWorkflows(MySQLGetFinishedWorkflows):
    pass
