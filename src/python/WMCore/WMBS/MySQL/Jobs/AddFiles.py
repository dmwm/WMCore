#!/usr/bin/env python
"""
_AddFiles_
MySQL implementation of Jobs.AddFiles
"""
__all__ = []
__revision__ = "$Id: AddFiles.py,v 1.6 2008/11/24 21:47:05 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddFiles(DBFormatter):
    sql = "insert into wmbs_job_assoc (job, file) values (:jobid, :fileid)"
    
    def format(self, result):
        return True
               
    def execute(self, id=0, file=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id, fileid=file)
        
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)