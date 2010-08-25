"""
_IsComplete_
MySQL implementation of JobGroup.IsComplete

Checks all the jobs are completed in  the given Job Group
"""
__all__ = []
__revision__ = "$Id: IsComplete.py,v 1.1 2009/03/24 21:59:33 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class IsComplete(DBFormatter):
    sql = """select 
                (select count(id) from wmbs_job where jobgroup=:jobgroup 
                 ) as ac, 
                (select count(job) from wmbs_group_job_complete where jobgroup=:jobgroup
                ) as cm 
            from dual
          """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0]
        
    def execute(self, group=None, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=group)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
