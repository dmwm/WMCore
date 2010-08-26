#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Workflow.LoadFromName
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2009/08/13 21:44:35 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID

class LoadFromName(LoadFromID):
    sql = """SELECT id, spec, name, owner, task FROM wmbs_workflow
             WHERE name = :workflow"""
