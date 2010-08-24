#!/usr/bin/env python
"""
_AddFiles_
MySQL implementation of Jobs.AddFiles
"""

__all__ = []
__revision__ = "$Id: AddFiles.py,v 1.7 2009/01/11 17:44:41 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddFiles(DBFormatter):
    sql = "INSERT INTO wmbs_job_assoc (job, file) VALUES (:jobid, :fileid)"
    
    def format(self, result):
        return True
               
    def execute(self, id=0, file=0, conn = None, transaction = False):
        binds = self.getBinds(jobid = id, fileid=file)
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)        
        return self.format(result)
