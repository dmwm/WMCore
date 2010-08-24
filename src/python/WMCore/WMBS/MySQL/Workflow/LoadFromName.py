#!/usr/bin/env python
"""
_Load_

MySQL implementation of Workflow.Load

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/07/03 09:43:56 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID

class LoadFromName(LoadFromID):
    sql = """select id, spec, name, owner from wmbs_workflow where name = :workflow"""
    