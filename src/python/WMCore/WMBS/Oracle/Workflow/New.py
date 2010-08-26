#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/05/08 16:38:21 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = """insert into wmbs_workflow (id, spec, owner, name, task)
             values (wmbs_workflow_SEQ.nextval, :spec, :owner, :name, :task)"""
    