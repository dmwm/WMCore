#!/usr/bin/env python
"""
_GetDeletableWorkflows_

Oracle implementation of Workflow.GetDeletableWorkflows

"""

from WMCore.WMBS.MySQL.Workflow.GetDeletableWorkflows import GetDeletableWorkflows as MySQLGetDeletableWorkflows

class GetDeletableWorkflows(MySQLGetDeletableWorkflows):
    pass
