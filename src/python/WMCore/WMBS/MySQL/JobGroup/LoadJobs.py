#!/usr/bin/env python
"""
_LoadJobs_

MySQL implementation of JobGroup.LoadJobs
"""

__all__ = []
__revision__ = "$Id: LoadJobs.py,v 1.3 2009/01/14 16:45:16 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadJobs(DBFormatter):
    sql = "SELECT id FROM wmbs_job WHERE jobgroup = :jobgroup"

    def execute(self, jobgroup, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"jobgroup": jobgroup},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
