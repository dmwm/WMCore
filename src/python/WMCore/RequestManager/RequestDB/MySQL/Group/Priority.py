#!/usr/bin/env python
"""
_Groups.Priority_

MySQL implementation for adjusting group priorities

"""





from WMCore.Database.DBFormatter import DBFormatter

class Priority(DBFormatter):
    """
    _Priority_

    Modify the priority of a group by some +ve or -ve integer

    """
    def execute(self, group, priorityModifier, conn = None, trans = False):
        """
        _execute_

        Modify priority of named group by the priority modifier

        - *group* : Name of group
        - *priorityModifier* : value to change priority

        Positive modifier increases priority, negative reduces it

        """

        self.sql = "UPDATE reqmgr_group SET group_base_priority="
        self.sql += "group_base_priority+%i " % priorityModifier
        self.sql += "WHERE group_name=\'%s\'" % group

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        return self.format(result)





