#!/usr/bin/env python
"""
_LoadSpecOwner_

SQLite implementation of Workflow.LoadSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadSpecOwner.py,v 1.3 2008/10/16 15:39:13 jcgon Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadSpecOwner import LoadSpecOwner as LoadWorkflowMySQL

class LoadSpecOwner(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql
