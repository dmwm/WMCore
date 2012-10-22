#!/usr/bin/env python
"""
_ID_

Get a users info within the request system

"""







from WMCore.Database.DBFormatter import DBFormatter


class GetUserInfo(DBFormatter):
    """
    _ID_

    Get a HN username from the ID

    """

    def execute(self, userName, conn = None, trans = False):
        """
        _execute_

        Retrieve the HN username of the user defined by ID

        """
        self.sql = "SELECT requestor_hn_name, contact_email FROM reqmgr_requestor "
        self.sql += "WHERE requestor_hn_name=:user_name"
        binds = {"user_name": userName}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return {'contact_email': output[0][1]}
