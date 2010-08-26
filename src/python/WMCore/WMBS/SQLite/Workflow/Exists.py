#!/usr/bin/env python
"""
_Exists_

SQLite implementation of WorkflowExists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.Exists import Exists as WorkflowExistsMySQL

class Exists(WorkflowExistsMySQL):
    sql = WorkflowExistsMySQL.sql