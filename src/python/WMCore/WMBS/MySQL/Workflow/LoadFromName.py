#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Workflow.LoadFromName
"""

__all__ = []



from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID

class LoadFromName(LoadFromID):
    sql = """SELECT id, spec, name, owner, task FROM wmbs_workflow
             WHERE name = :workflow"""
