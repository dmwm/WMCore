#!/usr/bin/env python
"""
_DeleteWorkflow_

SQLite implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/12 10:02:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.Delete import Delete as DeleteWorkflowMySQL

class Delete(DeleteWorkflowMySQL, SQLiteBase):
    sql = DeleteWorkflowMySQL.sql
    