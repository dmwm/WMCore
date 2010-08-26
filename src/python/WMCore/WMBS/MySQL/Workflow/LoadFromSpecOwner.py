#!/usr/bin/env python
"""
_LoadFromSpecOwner_

MySQL implementation of Workflow.LoadFromSpecOwner
"""

__all__ = []
__revision__ = "$Id: LoadFromSpecOwner.py,v 1.5 2009/08/13 21:44:35 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
    
class LoadFromSpecOwner(DBFormatter):
    sql = """SELECT id, spec, name, owner, task FROM wmbs_workflow
             WHERE spec = :spec and owner = :owner"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id attribute to an int because the DBFormatter turns everything
        into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        return formattedResult
    
    def execute(self, spec = None, owner = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec, "owner": owner},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
