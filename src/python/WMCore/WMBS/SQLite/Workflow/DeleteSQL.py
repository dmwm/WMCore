#!/usr/bin/env python
"""
_DeleteWorkflow_

SQLite implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: DeleteSQL.py,v 1.1 2008/06/09 16:23:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.DeleteSQL import Delete as DeleteWorkflowMySQL

class Delete(DeleteWorkflowMySQL, SQLiteBase):
    sql = DeleteWorkflowMySQL.sql
    