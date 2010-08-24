#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.7 2009/01/11 17:45:52 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = "insert into wmbs_job (jobgroup, name) values (:jobgroup, :name)"
        
    def execute(self, jobgroup, name, conn = None, transaction = False):
        binds = self.getBinds(jobgroup = jobgroup, name = name)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
