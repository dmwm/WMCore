#!/usr/bin/env python
"""
_Exists_

SQLite implementation of WorkflowExists

"""
__all__ = []
__revision__ = "$Id: ExistsSQL.py,v 1.1 2008/06/09 16:23:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.ExistsSQL import Exists as WorkflowExistsMySQL

class Exists(WorkflowExistsMySQL, SQLiteBase):
    sql = WorkflowExistsMySQL.sql