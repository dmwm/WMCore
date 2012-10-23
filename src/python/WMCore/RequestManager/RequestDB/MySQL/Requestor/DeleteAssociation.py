#!/usr/bin/env python
"""
_User.DeleteAssociation_

Delete a user/group association

"""




from WMCore.Database.DBFormatter import DBFormatter


class DeleteAssociation(DBFormatter):
    """
    _DeleteAssociation_

    Delete an association between a requestor and group given the
    requestor and group DB ids

    """

    def execute(self, requestorId, groupId, conn = None, trans = False):
        """
        _execute_

        Remove association between user and group

        """
        self.sql = "DELETE FROM reqmgr_group_association WHERE "
        self.sql += " requestor_id = :requestor_id AND group_id = :group_id"
        binds = {"requestor_id":requestorId, "group_id": groupId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
