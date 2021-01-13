#!/usr/bin/env python
"""
_NewState_

MySQL implementation for labeling a job Complete
"""


from WMCore.Database.DBFormatter import DBFormatter

class NewState(DBFormatter):
    """
    _NewState_

    Insert new states into bl_status
    """


    sql = """INSERT IGNORE INTO bl_status (name) VALUES (:name)"""



    def execute(self, states, conn = None, transaction = False):
        """
        _execute_

        Complete jobs
        """

        binds = []
        if isinstance(states, str):
            binds = {'name': states}
        for state in states:
            binds.append({'name': state})


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return
