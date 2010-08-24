#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Subscription.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.2 2009/01/16 22:38:01 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, fileset, workflow, split_algo, type, last_update
             FROM wmbs_subscription WHERE id = :id"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, fileset, workflow and last_update columns to integers
        since formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["fileset"] = int(formattedResult["fileset"])
        formattedResult["workflow"] = int(formattedResult["workflow"])
        formattedResult["last_update"] = int(formattedResult["last_update"])        
        return formattedResult
    
    def execute(self, id = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"id": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
