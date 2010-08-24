#!/usr/bin/env python
"""
_LoadSpecOwner_

SQLite implementation of Workflow.LoadSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.1 2008/11/20 22:28:21 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner as LoadWorkflowMySQL

class LoadFromSpecOwner(LoadWorkflowMySQL, SQLiteBase):
    sql = LoadWorkflowMySQL.sql
