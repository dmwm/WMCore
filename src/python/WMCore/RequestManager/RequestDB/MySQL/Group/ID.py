#!/usr/bin/env python
"""
_ID_

Get a physics group ID within the request system

"""






from WMCore.Database.DBFormatter import DBFormatter


class ID(DBFormatter):
    """
    _ID_

    Get a group ID from the group name

    """

    def execute(self, groupname, conn = None, trans = False):
        """
        _execute_

        Retrieve the ID of the user defined by HN username

        """
        self.sql = "SELECT group_id FROM reqmgr_group "
        self.sql += "WHERE group_name=\'%s\'" % groupname

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]


