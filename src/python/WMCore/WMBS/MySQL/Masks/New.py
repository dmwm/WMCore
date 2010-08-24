#!/usr/bin/env python
"""
_New_

MySQL implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/01/11 17:44:41 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_job_mask (job, inclusivemask) VALUES
             (:jobid, :inclusivemask)"""
    
    def format(self,result):
        return True
    
    def execute(self, jobid, inclusivemask = None, conn = None,
                transaction = False):
        if inclusivemask == None:
            binds = self.getBinds(jobid = jobid, inclusivemask=True)
        else:
            binds = self.getBinds(jobid = jobid, inclusivemask = inclusivemask)
            
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
