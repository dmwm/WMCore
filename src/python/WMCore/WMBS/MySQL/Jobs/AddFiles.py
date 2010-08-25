#!/usr/bin/env python
"""
_AddFiles_
MySQL implementation of Jobs.AddFiles
"""

__all__ = []
__revision__ = "$Id: AddFiles.py,v 1.8 2009/03/18 19:57:56 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddFiles(DBFormatter):
    sql = """INSERT INTO wmbs_job_assoc (job, file)
               SELECT :jobid, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT * FROM wmbs_job_assoc
                  WHERE job = :jobid AND file = :fileid)"""
    
    def execute(self, id = None, file = None, conn = None, transaction = False):
        binds = self.getBinds(jobid = id, fileid = file)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)        
        return self.format(result)
