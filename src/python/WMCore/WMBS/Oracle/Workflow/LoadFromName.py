#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of Workflow.LoadFromName

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/10/08 14:30:12 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName as LoadWorkflowMySQL

class LoadFromID(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql
    