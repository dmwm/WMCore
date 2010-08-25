#!/usr/bin/env python
"""
_User.NewAssociation_

Insert a user/group association

"""




from WMCore.Database.DBFormatter import DBFormatter


class NewAssociation(DBFormatter):
    """
    _NewAssociation_

    Add an association between a requestor and group given the
    requestor and group DB ids

    """

    def execute(self, requestorId, groupId, conn = None, trans = False):
        """
        _execute_

        Add new association between user and group

        """
        self.sql = "INSERT INTO reqmgr_group_association( "
        self.sql += "requestor_id, group_id) VALUES ( %s, %s)" % (
            requestorId, groupId)
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

