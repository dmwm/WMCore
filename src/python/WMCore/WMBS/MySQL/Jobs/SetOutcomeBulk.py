#!/usr/bin/env python
"""
_SetOutcomeBulk_

MySQL implementation of Jobs.SetOutcomeBulk
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class SetOutcomeBulk(DBFormatter):
    sql = """UPDATE wmbs_job SET outcome = :outcome
               WHERE id = :jobid"""

    def execute(self, binds, conn = None, transaction = False):
        """
        _execute_

        Expect a list of binds of the type:
        {'jobid': ID, 'outcome': outcome}
        """
        for bind in binds:
            if bind['outcome'] == 'success' \
                   or bind['outcome'] == '1' \
                   or bind['outcome'] == 1:
                bind['outcome'] = 1
            else:
                bind['outcome'] = 0

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
