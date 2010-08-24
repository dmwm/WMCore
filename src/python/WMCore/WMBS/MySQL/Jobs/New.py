#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.8 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_job (jobgroup, name, last_update)
             VALUES (:jobgroup, :name, %d)""" % time.time()
        
    def execute(self, jobgroup, name, conn = None, transaction = False):
        binds = self.getBinds(jobgroup = jobgroup, name = name)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
