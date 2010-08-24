#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Workflow.LoadFromName
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.2 2009/01/14 16:41:50 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID

class LoadFromName(LoadFromID):
    sql = """SELECT id, spec, name, owner FROM wmbs_workflow
             WHERE name = :workflow"""
