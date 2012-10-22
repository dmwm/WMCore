#!/usr/bin/env python
"""
_Group.New_

Action to insert a new group into ReqMgr

"""



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):



    def execute(self, groupName, groupPriority,
                conn = None, transaction = False):

        self.sql = "INSERT INTO reqmgr_group ( "
        self.sql += "group_name,group_base_priority  )"
        self.sql += " VALUES (:group_name, :group_priority)"
        binds = {"group_name": groupName, "group_priority": int(groupPriority)}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
