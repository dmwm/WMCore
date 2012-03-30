#!/usr/bin/env python
"""
_CheckInjectedWorkflow_

Oracle implementation of Workflow.CheckInjectedWorkflow
"""

from WMCore.WMBS.MySQL.Workflow.CheckInjectedWorkflow import CheckInjectedWorkflow as MySQLCheckInjectedWorkflow

class CheckInjectedWorkflow(MySQLCheckInjectedWorkflow):
    """
    Oracle version

    """
