#!/usr/bin/env python
"""
_LoadFromSpecOwner_

MySQL implementation of Workflow.LoadFromSpecOwner
"""

__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.3 2009/01/14 16:41:11 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
    
class LoadFromSpecOwner(DBFormatter):
    sql = """SELECT id, spec, name, owner FROM wmbs_workflow
             WHERE spec = :spec and owner = :owner"""
    
    def execute(self, spec = None, owner = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec, "owner": owner},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
