#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Workflow.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.5 2009/08/13 21:44:35 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = "SELECT id, spec, name, owner, task FROM wmbs_workflow WHERE id = :workflow"

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id attribute to an int because the DBFormatter turns everything
        into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        return formattedResult
                                    
    def execute(self, workflow = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
