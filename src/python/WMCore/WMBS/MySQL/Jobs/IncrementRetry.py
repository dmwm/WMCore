#!/usr/bin/env python
"""
_IncrementRetry_

"""

__revision__ = "$Id: IncrementRetry.py,v 1.1 2010/07/13 22:11:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class IncrementRetry(DBFormatter):
    select = "SELECT id, retry_count FROM wmbs_job WHERE id = :job"
    update = "UPDATE wmbs_job SET retry_count = :retry WHERE id = :job"
    
    def execute(self, jobs = [], conn = None, transaction = False):
        binds = []
        for job in jobs:
            binds.append({"job": job["id"]})
            
        results = self.dbi.processData(self.select, binds, conn = conn,
                                       transaction = transaction)

        updateBinds = []
        for result in self.formatDict(results):
            updateBinds.append({"job": result["id"],
                                "retry": int(result["retry_count"]) + 1})

        self.dbi.processData(self.update, updateBinds, conn = conn,
                             transaction = transaction)
        return
