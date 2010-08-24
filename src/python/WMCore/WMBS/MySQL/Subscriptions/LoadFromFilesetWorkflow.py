#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

MySQL implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []
__revision__ = "$Id: LoadFromFilesetWorkflow.py,v 1.1 2009/01/14 16:35:25 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromFilesetWorkflow(DBFormatter):
    sql = """SELECT id, fileset, workflow, split_algo, type, last_update
             FROM wmbs_subscription WHERE fileset = :fileset
             AND workflow = :workflow"""
    
    def execute(self, fileset = None, workflow = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset,
                                                 "workflow": workflow},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
