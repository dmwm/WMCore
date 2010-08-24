#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Workflow.LoadFromName

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.2 2008/11/24 21:51:56 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName as LoadWorkflowMySQL

class LoadFromName(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
    