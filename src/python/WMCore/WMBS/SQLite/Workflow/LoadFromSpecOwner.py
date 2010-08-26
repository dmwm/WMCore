#!/usr/bin/env python
"""
_LoadFromSpecOwner_

SQLite implementation of Workflow.LoadFromSpecOwner

"""

__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.2 2008/11/26 19:52:02 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner as LoadWorkflowMySQL

class LoadFromSpecOwner(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
