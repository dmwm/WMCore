#!/usr/bin/env python
"""
_Groups.SetPriority_

MySQL implementation for adjusting group priorities

"""





from WMCore.Database.DBFormatter import DBFormatter

class SetPriority(DBFormatter):
    """
    _SetPriority_

    Modify the priority of a group

    """
    def execute(self, groupName, priority, conn = None, trans = False):
        """
        _execute_

        Modify priority of named group by the priority modifier

        - *group* : Name of group
        - *priority* : value to change priority


        """

        self.sql = "UPDATE reqmgr_group SET group_base_priority=:priority"
        self.sql += " WHERE group_name=:group_name"
        binds = {"priority": int(priority), "group_name": groupName}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = trans)
        return self.format(result)
