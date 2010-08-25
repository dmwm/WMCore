#!/usr/bin/env python
"""
_ID_

Get a users ID within the request system

"""







from WMCore.Database.DBFormatter import DBFormatter


class NameFromID(DBFormatter):
    """
    _ID_

    Get a HN username from the ID

    """

    def execute(self, id, conn = None, trans = False):
        """
        _execute_

        Retrieve the HN username of the user defined by ID

        """
        self.sql = "SELECT requestor_hn_name FROM reqmgr_requestor "
        self.sql += "WHERE requestor_id=\'%s\'" % id

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]





