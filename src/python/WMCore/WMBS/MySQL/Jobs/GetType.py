#!/usr/bin/env python
"""
_GetType_

MySQL implementation of Jobs.GetType
"""

__revision__ = "$Id: GetType.py,v 1.1 2009/10/13 20:52:41 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetType(DBFormatter):
    """
    _GetType_

    Given a job ID, get the type of job from the subscription.
    """
    sql = """SELECT wmbs_sub_types.name FROM wmbs_sub_types
               INNER JOIN wmbs_subscription ON
                 wmbs_sub_types.id = wmbs_subscription.subtype
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
             WHERE wmbs_job.id = :jobid"""    

    def format(self, results):
        if len(results) == 0:
            return None
        else:
            return results[0].fetchall()[0].values()[0]

        
    def execute(self, jobID, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.format(result)
