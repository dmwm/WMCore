#!/usr/bin/env python
"""
_Request.Get_

API for getting a new request by its ID

"""



from WMCore.Database.DBFormatter import DBFormatter

class Get(DBFormatter):
    """
    _Get_

    retrieve the details of a request given a request id

    """
    def execute(self, requestId, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """
        self.sql = "select * from reqmgr_request where request_id = %s" % (
            requestId)


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        if len(result) == 0:
            return None
        value = result[0]
        if value == None:
            return None
        value = value.fetchone()
        requestData = {}
        for field, val in value.items():
            requestData[str(field).lower()] = val
        return requestData

