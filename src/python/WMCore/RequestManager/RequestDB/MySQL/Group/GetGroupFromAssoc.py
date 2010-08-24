#!/usr/bin/env python
"""
_Request.GetGroupFromAssoc_

API for getting the group from an association id

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetGroupFromAssoc(DBFormatter):
    """
    _GetGroupFromAssoc_

    retrieve the group details given an association id

    """
    def execute(self, associationId, conn = None, trans = False):
        """
        _execute_

        get the details about a group from an association id

        """
        self.sql = """select * from reqmgr_group
       JOIN reqmgr_group_association
         ON reqmgr_group_association.group_id = reqmgr_group.group_id
           WHERE reqmgr_group_association.association_id = %s
           """ % associationId


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        if len(result) == 0:
            return None
        row = result[0].fetchone()
        groupInfo = dict(zip([str(x).lower() for x in row.keys()], row.values()))
        return groupInfo
