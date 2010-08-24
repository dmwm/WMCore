#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/24 21:51:55 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = """insert into wmbs_workflow (id, spec, owner, name)
             values (wmbs_workflow_SEQ.nextval, :spec, :owner, :name)"""
    