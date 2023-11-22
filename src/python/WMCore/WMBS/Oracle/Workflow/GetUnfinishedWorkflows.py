#!/usr/bin/env python
"""
Oracle implementation of Workflow.GetUnfinishedWorkflows
"""

from WMCore.WMBS.MySQL.Workflow.GetUnfinishedWorkflows import \
    GetUnfinishedWorkflows as MySQLGetUnfinishedWorkflows


class GetUnfinishedWorkflows(MySQLGetUnfinishedWorkflows):
    pass
