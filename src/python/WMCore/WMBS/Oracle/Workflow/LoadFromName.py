#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Workflow.LoadFromName

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName \
     as LoadWorkflowMySQL

class LoadFromName(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
    