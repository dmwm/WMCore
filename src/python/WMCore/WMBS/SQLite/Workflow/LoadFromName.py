#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of Workflow.LoadFromName

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.2 2008/10/09 09:56:39 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName as LoadWorkflowMySQL

class LoadFromName(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql
    