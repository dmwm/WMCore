#!/usr/bin/env python
"""
_Requestor.Priority_

Adjust user priority

"""






from WMCore.Database.DBFormatter import DBFormatter


class Priority(DBFormatter):


    def execute(self, username, priorityModifier, conn = None, trans = False):
        """
        _execute_

        Update priority for user with username, with modifier provided

        - *username* : HN username of user
        - *priorityModifier* : value to change priority

        Positive modifier increases priority, negative reduces it
        """

        self.sql = "UPDATE reqmgr_requestor SET requestor_base_priority="
        self.sql += "requestor_base_priority+%i " % priorityModifier
        self.sql += "WHERE requestor_hn_name=\'%s\'" % username

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return self.format(result)

