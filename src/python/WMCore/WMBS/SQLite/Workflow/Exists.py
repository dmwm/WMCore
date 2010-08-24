#!/usr/bin/env python
"""
_Exists_

SQLite implementation of WorkflowExists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/06/12 10:02:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.Exists import Exists as WorkflowExistsMySQL

class Exists(WorkflowExistsMySQL, SQLiteBase):
    sql = WorkflowExistsMySQL.sql