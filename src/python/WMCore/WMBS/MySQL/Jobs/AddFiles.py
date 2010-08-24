#!/usr/bin/env python
"""
_AddFiles_
MySQL implementation of Jobs.AddFiles
"""
__all__ = []
__revision__ = "$Id: AddFiles.py,v 1.1 2008/08/05 17:59:30 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class AddFiles(MySQLBase):
    sql = "insert into wmbs_job_assoc (id, file) values (:jobid, :fileid)"
               
    def execute(self, id=0, file=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id, fileid=file)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)