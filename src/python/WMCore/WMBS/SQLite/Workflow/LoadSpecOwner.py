#!/usr/bin/env python
"""
_LoadFromSpecOwner_

SQLite implementation of Workflow.LoadFromSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadSpecOwner.py,v 1.2 2008/10/09 09:56:39 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner as LoadWorkflowMySQL

class LoadSpecOwner(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql