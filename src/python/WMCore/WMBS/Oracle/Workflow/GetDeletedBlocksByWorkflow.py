#!/usr/bin/env python
"""
_GetDeletedBlocksByWorkflow_

Oracle implementation of Workflow.GetDeletedBlocksByWorkflow

NOTE: This DAO is not used in the production code but is to be used only for debugging purposes
"""

from WMCore.WMBS.MySQL.Workflow.GetDeletedBlocksByWorkflow import GetDeletedBlocksByWorkflow as MySQLGetDeletedBlocksByWorkflow


class GetDeletedBlocksByWorkflow(MySQLGetDeletedBlocksByWorkflow):
    """
    Retrieves a list of all workflows and the relative deleted blocks lists
    """
