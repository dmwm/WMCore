#!/usr/bin/env python
"""
_LoadSpecOwner_

SQLite implementation of Workflow.LoadSpecOwner

"""
__all__ = []
__revision__ = "$Id: LoadSpecOwner.py,v 1.4 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Workflow.LoadSpecOwner import LoadSpecOwner as LoadWorkflowMySQL

class LoadSpecOwner(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
