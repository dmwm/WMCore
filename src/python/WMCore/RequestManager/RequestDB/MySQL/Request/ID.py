#!/usr/bin/env python
"""
_Request.ID_

API for getting a request id from its name

"""



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.DBFormatter import DBFormatter

class ID(DBFormatter):
    """
    _ID_

    retrieve the details of a request given a request id

    """
    def execute(self, requestName, conn = None, trans = False):
        """
        _execute_

        map the request name provided to an ID

        """
        self.sql = "SELECT request_id from reqmgr_request WHERE "
        self.sql += "request_name=\'%s\'" % requestName
        result = self.executeOne(conn = conn, transaction = trans)
        if result == []:
            return None
        return result[0]

