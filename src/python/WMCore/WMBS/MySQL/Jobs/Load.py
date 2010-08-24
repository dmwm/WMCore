#!/usr/bin/env python
"""
_Load_
MySQL implementation of Jobs.Load
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.5 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    sql = "select file from wmbs_job_assoc where job = :jobid"
    
    def format(self, result):
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i[0])
        return out
               
    def execute(self, id=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
