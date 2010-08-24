#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Workflow.LoadFromID

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/07/21 14:21:07 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID as LoadWorkflowMySQL

class LoadFromID(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql 