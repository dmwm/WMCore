#!/usr/bin/env python
"""
_Groups.GetPriority_

MySQL implementation for getting group priorities

"""





from WMCore.Database.DBFormatter import DBFormatter

class GetPriority(DBFormatter):
    """
    _GetPriority_

    Get the priority of a group

    """
    def execute(self, group, conn = None, trans = False):
        """
        _execute_

        Get priority of named group

        - *group* : Name of group

        """

        self.sql = "SELECT group_base_priority FROM reqmgr_group WHERE group_name=:group_name"
        binds = {"group_name": group}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = trans)
        return self.formatOne(result)[0]
