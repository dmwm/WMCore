#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of Workflow.LoadFromName

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName as LoadWorkflowMySQL

class LoadFromName(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
    