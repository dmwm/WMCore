#!/usr/bin/env python
"""
_DeleteWorkflow_

Oracle implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/11/24 21:51:56 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.Delete import Delete as DeleteWorkflowMySQL

class Delete(DeleteWorkflowMySQL):
    sql = DeleteWorkflowMySQL.sql
    