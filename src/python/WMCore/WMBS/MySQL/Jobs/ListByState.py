#!/usr/bin/env python
"""
_ListByState_

MySQL implementation of ListByState

Created on July 10, 2013

@author: tsarangi
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListByState(DBFormatter):
    """
    DAO to list jobs given certain state only,
    """
    sql = """SELECT wmbs_job.id, wmbs_job.retry_count,
                    wmbs_job.cache_dir
                FROM wmbs_job
                INNER JOIN wmbs_job_state ON
                    wmbs_job.state = wmbs_job_state.id
                 WHERE wmbs_job_state.name = :state"""

    def execute(self, state, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {'state' : state},
                                      conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
