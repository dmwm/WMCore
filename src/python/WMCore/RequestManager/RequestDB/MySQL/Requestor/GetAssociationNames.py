#!/usr/bin/env python
"""
_User.GetAssociationNames_

Retrieve a user/groupname association

"""




from WMCore.Database.DBFormatter import DBFormatter


class GetAssociationNames(DBFormatter):
    """
    _GetAssociationNames_

    Get the associations between a requestor and groups given the
    requestor DB id.


    """

    def execute(self, requestorId, conn = None, trans = False):
        """
        _execute_

        get a map of group name to association id for the requestor ID
        provided

        """


        self.sql = """
        select reqmgr_group.group_name,reqmgr_group_association.association_id
         from reqmgr_group
          JOIN reqmgr_group_association
             ON reqmgr_group_association.group_id = reqmgr_group.group_id
               WHERE reqmgr_group_association.requestor_id=%s""" % (
            requestorId, )


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))


