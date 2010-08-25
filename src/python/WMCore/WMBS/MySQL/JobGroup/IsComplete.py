"""
_IsComplete_
MySQL implementation of JobGroup.IsComplete

Checks all the jobs are completed in  the given Job Group
"""
__all__ = []
__revision__ = "$Id: IsComplete.py,v 1.2 2009/04/29 23:16:59 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class IsComplete(DBFormatter):
    sql = """select 
                (select count(id) from wmbs_job where jobgroup=:jobgroup 
                 ) as ac, 
                (select count(job) from wmbs_group_job_complete where jobgroup=:jobgroup
                ) as cm,
                (select count(job) from wmbs_group_job_failed where jobgroup=:jobgroup
                ) as fa 
             from dual
          """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0]
        
    def execute(self, group, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=group)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
