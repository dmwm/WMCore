#!/usr/bin/env python
"""
_Group.GetAssociation_

Retrieve a user/group association

"""




from WMCore.Database.DBFormatter import DBFormatter


class GetAssociation(DBFormatter):
    """
    _GetAssociation_

    Get the associations between a requestor and groups given the
    group DB id.
    A list of requestor IDs is returned.

    """

    def execute(self, requestorId, conn = None, trans = False):
        """
        _execute_



        """
        self.sql = "SELECT requestor_id FROM reqmgr_group_association "
        self.sql += " WHERE group_id = %s " % (
            requestorId,)
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        values = [ x[0] for x in self.format(result)]
        return values

