#!/usr/bin/env python
"""
_GetInjectedWorkflows_

Oracle implementation of Workflow.GetInjectedWorkflows

"""
__all__ = []

from WMCore.WMBS.MySQL.Workflow.GetInjectedWorkflows import GetInjectedWorkflows as MySQLGetInjectedWorkflows

class GetInjectedWorkflows(MySQLGetInjectedWorkflows):
    """
    Oracle Template

    """
