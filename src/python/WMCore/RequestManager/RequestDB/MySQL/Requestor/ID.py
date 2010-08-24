#!/usr/bin/env python
"""
_ID_

Get a users ID within the request system

"""







from WMCore.Database.DBFormatter import DBFormatter


class ID(DBFormatter):
    """
    _ID_

    Get a users ID from the HN username

    """

    def execute(self, username, conn = None, trans = False):
        """
        _execute_

        Retrieve the ID of the user defined by HN username

        """
        self.sql = "SELECT requestor_id FROM reqmgr_requestor "
        self.sql += "WHERE requestor_hn_name=\'%s\'" % username

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]





