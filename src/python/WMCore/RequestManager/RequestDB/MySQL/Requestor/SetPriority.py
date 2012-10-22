#!/usr/bin/env python
"""
_Requestor.SetPriority_

Adjust user priority

"""






from WMCore.Database.DBFormatter import DBFormatter


class SetPriority(DBFormatter):


    def execute(self, username, priority, conn = None, trans = False):
        """
        _execute_

        Update priority for user with username, to value provided

        - *username* : HN username of user
        - *priority* : value to change priority to
        """

        self.sql = "UPDATE reqmgr_requestor SET requestor_base_priority=:priority"
        self.sql += " WHERE requestor_hn_name=:username"
        binds = {"priority": int(priority), "username": username}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
