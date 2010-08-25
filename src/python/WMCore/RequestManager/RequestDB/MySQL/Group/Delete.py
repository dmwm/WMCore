#!/usr/bin/env python
"""
_Group.Delete_

Action to delete a group from WMCore.RequestManager

"""



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete a group from the DB

    """
    def execute(self, groupName, conn = None, transaction = False):
        """
        _execute_

        Delete the named group from the database

        """
        self.sql = "DELETE FROM reqmgr_group WHERE group_name=\'%s\'" % (
            groupName,)

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = transaction)
        return self.format(result)

