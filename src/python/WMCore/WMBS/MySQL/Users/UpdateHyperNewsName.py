#!/usr/bin/env python
"""
_UpdateHyperNewsName_

MySQL implementation for updating user hyper news name
"""


from WMCore.Database.DBFormatter import DBFormatter

class UpdateHyperNewsName(DBFormatter):
    """
    _UpdateHyperNewsName_

    Update user hyper news name
    """


    sql = """UPDATE wmbs_users SET name_hn = :name_hn
               WHERE id = :id
               """



    def execute(self, id, name_hn, conn = None, transaction = False):
        """
        _execute_

        Update user hyper news name.
        """

        binds = {'id': id, 'name_hn': name_hn}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return
