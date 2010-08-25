#!/usr/bin/env python
"""
_LoadFromTask_

SQLite implementation of Workflow.LoadFromTask
"""

__all__ = []
__revision__ = "$Id: LoadFromTask.py,v 1.1 2010/06/01 17:22:39 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromTask import LoadFromTask as LoadFromTaskMySQL

class LoadFromName(LoadFromTaskMySQL):
    sql = LoadFromTaskMySQL.sql

