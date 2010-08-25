#!/usr/bin/env python
"""
_Request.GetUserFromAssoc_

API for getting the requestor from an association id

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetUserFromAssoc(DBFormatter):
    """
    _GetUserFromAssoc_

    retrieve the requestor details given an association id

    """
    def execute(self, associationId, conn = None, trans = False):
        """
        _execute_

        get the details about a requestor from an association id

        """
        self.sql = """select * from reqmgr_requestor
       JOIN reqmgr_group_association
         ON reqmgr_group_association.requestor_id = reqmgr_requestor.requestor_id
           WHERE reqmgr_group_association.association_id = %s
           """ % associationId


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        if len(result) == 0:
            return None
        row = result[0].fetchone()
        userInfo = dict(zip([str(x).lower() for x in row.keys()], row.values()))
        return userInfo
