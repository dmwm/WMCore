#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

MySQL implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []
__revision__ = "$Id: LoadFromFilesetWorkflow.py,v 1.2 2009/01/16 22:38:01 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromFilesetWorkflow(DBFormatter):
    sql = """SELECT id, fileset, workflow, split_algo, type, last_update
             FROM wmbs_subscription WHERE fileset = :fileset
             AND workflow = :workflow"""

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
    
    def execute(self, fileset = None, workflow = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset,
                                                 "workflow": workflow},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
