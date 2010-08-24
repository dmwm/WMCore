#!/usr/bin/env python
"""
_NewWorkflow_

SQLite implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: NewSQL.py,v 1.1 2008/06/09 16:23:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.NewSQL import New as NewWorkflowMySQL

class New(NewWorkflowMySQL, SQLiteBase):
    sql = NewWorkflowMySQL.sql
    