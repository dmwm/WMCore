#!/usr/bin/env python
"""
_LoadFromSpecOwner_

SQLite implementation of Workflow.LoadFromSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.1 2008/11/24 21:51:56 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner as LoadWorkflowMySQL

class LoadFromSpecOwner(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql