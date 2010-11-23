#!/usr/bin/env python
"""
_KillWorkflow_

Oracle implementation of Subscriptions.KillWorkflow
"""

from WMCore.WMBS.MySQL.Subscriptions.KillWorkflow import KillWorkflow as MySQLKillWorkflow

class KillWorkflow(MySQLKillWorkflow):
    """
    _KillWorkflow_

    Mark all files that are not complete/failed and belong to a particular
    workflow as failed.  Ignore Cleanup and LogCollect subscriptions as we
    still want those to run.
    """
    pass
