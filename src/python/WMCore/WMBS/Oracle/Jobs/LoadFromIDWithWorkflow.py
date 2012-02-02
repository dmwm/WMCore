#!/usr/bin/env python
"""
_LoadFromIDWithWorkflow_

Oracle implementation of Jobs.LoadFromIDWithWorkflow.
"""

from WMCore.WMBS.MySQL.Jobs.LoadFromIDWithWorkflow import LoadFromIDWithWorkflow as MySQLLoadFromIDWithWorkflow

class LoadFromIDWithWorkflow(MySQLLoadFromIDWithWorkflow):
    """
    Load jobs by ID but include the workflow name

    """

    pass
