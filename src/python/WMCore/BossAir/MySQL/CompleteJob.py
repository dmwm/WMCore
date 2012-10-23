#!/usr/bin/env python
"""
_CompleteJob_

MySQL implementation for labeling a job Complete
"""

import logging

from WMCore.Database.DBFormatter import DBFormatter

class CompleteJob(DBFormatter):
    """
    _CompleteJob_

    Label jobs as complete
    """


    sql = """UPDATE bl_runjob SET status = '0' WHERE id = :id"""



    def execute(self, jobs, conn = None, transaction = False):
        """
        _execute_

        Complete jobs
        """

        if len(jobs) < 1:
            # Then we have nothing to do
            return

        binds = []
        for job in jobs:
            binds.append({'id': job})


        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)


        return
