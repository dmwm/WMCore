#!/usr/bin/env python
"""
_Load_
MySQL implementation of Jobs.Load
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/08/05 17:59:30 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Load(MySQLBase):
    sql = "select file from wmbs_job_assoc where job = :jobid"
               
    def execute(self, id=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)