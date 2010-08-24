#!/usr/bin/env python
"""
_Load_
MySQL implementation of Jobs.Load
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.3 2008/10/01 15:58:37 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Load(MySQLBase):
    sql = "select file from wmbs_job_assoc where job = :jobid"
    
    def format(self, result):
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i[0])
        return out
               
    def execute(self, id=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)