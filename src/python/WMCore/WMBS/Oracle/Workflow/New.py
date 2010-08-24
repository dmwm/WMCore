#!/usr/bin/env python
"""
_NewWorkflow_

SQLite implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/10/08 14:30:11 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL, SQLiteBase):
    sql = NewWorkflowMySQL.sql
    