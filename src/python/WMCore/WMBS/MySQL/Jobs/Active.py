#!/usr/bin/env python
"""
_Active_

MySQL implementation of Jobs.Active
"""

__all__ = []
__revision__ = "$Id: Active.py,v 1.7 2009/03/20 14:29:19 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class Active(DBFormatter):
    insertSQL = """INSERT INTO wmbs_group_job_acquired (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       FROM dual WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_acquired WHERE job = :job)"""    
    
    def execute(self, job, conn = None, transaction = False):
        self.dbi.processData(self.insertSQL, {"job": job}, conn = conn,
                             transaction = transaction)        
        return 
