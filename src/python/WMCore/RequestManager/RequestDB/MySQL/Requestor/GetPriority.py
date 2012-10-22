#!/usr/bin/env python
"""
_Requestor.Priority_

Get user priority

"""






from WMCore.Database.DBFormatter import DBFormatter


class GetPriority(DBFormatter):


    def execute(self, username, conn = None, trans = False):
        """
        _execute_

        get priority for user with username

        - *username* : HN username of user
        """

        self.sql = "SELECT requestor_base_priority FROM reqmgr_requestor "
        self.sql += "WHERE requestor_hn_name=:username"
        binds = {"username": username}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.formatOne(result)[0]
