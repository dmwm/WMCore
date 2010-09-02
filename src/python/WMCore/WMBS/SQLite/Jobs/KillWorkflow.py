#!/usr/bin/env python
"""
_KillWorkflow_

SQLite implementation of Jobs.KillWorkflow
"""

from WMCore.WMBS.MySQL.Jobs.KillWorkflow import KillWorkflow as KillWorkflowMySQL

class KillWorkflow(KillWorkflowMySQL):
    pass
