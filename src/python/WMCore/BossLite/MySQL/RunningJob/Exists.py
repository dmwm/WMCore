#!/usr/bin/env python
"""
_Exists_

MySQL implementation of BossLite.Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2010/05/10 13:00:10 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    """
    BossLite.Jobs.Exists
    """
    
    sql = """SELECT id FROM bl_runningjob 
             WHERE job_id = :job_id AND 
                    task_id = :task_id AND 
                    submission = :submission """


    def execute(self, taskID, jobID, submission, 
                        conn = None, transaction = False):
        """
        Expects unique name
        """

        binds = {'job_id': jobID, 'task_id': taskID, 'submission': submission}
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        res = self.format(result)

        if res == []:
            return False
        else:
            return res[0][0]

        
