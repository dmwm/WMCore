#!/usr/bin/env python
"""
_LoadFromSpecOwner_

SQLite implementation of Workflow.LoadFromSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.2 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner \
     as LoadWorkflowMySQL

class LoadFromSpecOwner(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql