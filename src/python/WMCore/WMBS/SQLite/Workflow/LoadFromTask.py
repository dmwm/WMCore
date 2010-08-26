#!/usr/bin/env python
"""
_LoadFromTask_

SQLite implementation of Workflow.LoadFromTask
"""

__all__ = []
__revision__ = "$Id: LoadFromTask.py,v 1.2 2010/06/01 21:23:18 riahi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromTask import LoadFromTask as LoadFromTaskMySQL

class LoadFromTask(LoadFromTaskMySQL):
    sql = LoadFromTaskMySQL.sql

