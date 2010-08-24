#!/usr/bin/env python
"""
_LoadJobs_

MySQL implementation of JobGroup.LoadJobs
"""

__all__ = []
__revision__ = "$Id: LoadJobs.py,v 1.2 2009/01/12 16:49:29 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadJobs(DBFormatter):
    sql = "select id from wmbs_job where jobgroup = :jobgroup"

    def format(self, results):
        results = DBFormatter.format(self, results)        
        out = []
        for result in results:
            out.append(result[0])

        return out

    def execute(self, jobgroup, conn = None, transaction = False):
        binds = self.getBinds(jobgroup = jobgroup)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
