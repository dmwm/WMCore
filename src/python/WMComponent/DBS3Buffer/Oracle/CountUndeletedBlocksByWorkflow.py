#!/usr/bin/env python
"""
_CountUndeletedBlocksByWorkflow_

Oracle implementation of Workflow.CountUndeletedBlocksByWorkflow
"""

from WMComponent.DBS3Buffer.MySQL.CountUndeletedBlocksByWorkflow import CountUndeletedBlocksByWorkflow as MySQLCountUndeletedBlocksByWorkflow


class CountUndeletedBlocksByWorkflow(MySQLCountUndeletedBlocksByWorkflow):
    """
    Retrieves a list of all workflows and the relative deleted blocks counters
    """
