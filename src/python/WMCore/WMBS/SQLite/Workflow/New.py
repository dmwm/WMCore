#!/usr/bin/env python
"""
_NewWorkflow_

SQLite implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = NewWorkflowMySQL.sql
    