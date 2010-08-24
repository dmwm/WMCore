#!/usr/bin/env python
"""
_NewWorkflow_

SQLite implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/06/12 10:02:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL, SQLiteBase):
    sql = NewWorkflowMySQL.sql
    