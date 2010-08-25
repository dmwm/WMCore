#!/usr/bin/env python
"""
_Exists_

MySQL implementation of BossLite.Jobs.Exists
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    """
    BossLite.Jobs.Exists
    """
    
    sql = """SELECT id FROM bl_runningjob 
             WHERE job_id = :jobId AND 
                    task_id = :taskId AND 
                    submission = :submission """
    
    def execute(self, binds, conn = None, transaction = False):
        """
        Expects unique name
        """
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        res = self.format(result)

        if res == []:
            return False
        else:
            return res[0][0]

        
