#!/usr/bin/env python
"""
_User.DeleteAssociation_

Delete a user/group association

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DeleteAssociation.py,v 1.1 2010/07/01 19:14:15 rpw Exp $"


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
        self.sql += " requestor_id = %s AND group_id = %s" % (
            requestorId, groupId)
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

