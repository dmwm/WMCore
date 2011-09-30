#!/usr/bin/env python
"""
_MarkInjectedWorkflows_

Oracle implementation of Workflow.MarkInjectedWorkflows

"""
__all__ = []

from WMCore.WMBS.MySQL.Workflow.MarkInjectedWorkflows import MarkInjectedWorkflows as MySQLMarkInjectedWorkflows

class MarkInjectedWorkflows(MySQLMarkInjectedWorkflows):
    """
    Oracle Template

    """
