#!/usr/bin/env python
"""
_CountWorkflowBySpec_

Oracle implementation of Workflow.CountWorkflowBySpec
"""

from WMCore.WMBS.MySQL.Workflow.CountWorkflowBySpec import CountWorkflowBySpec as MySQLCountWorkflowBySpec

class CountWorkflowBySpec(MySQLCountWorkflowBySpec):
    """
    Oracle version

    """
