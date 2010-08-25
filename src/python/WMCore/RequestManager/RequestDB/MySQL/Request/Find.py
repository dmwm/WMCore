#!/usr/bin/env python
"""
_Request.Find_

API for finding a new request by status

"""



from WMCore.Database.DBFormatter import DBFormatter

class Find(DBFormatter):
    """
    _Find_

    Find all request names/ids in the database


    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        retrieve id/name pairs for all requests in the db
        returns a dictionay of { name: id, status }

        """
        self.sql = """
        SELECT req.request_name, req.request_id, stat.status_name
          FROM reqmgr_request req
            JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id
        """

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        output = self.format(result)
        requests = []
        [ requests.append(
            {'RequestName'   : x[0],
             'RequestID'     : x[1],
             'RequestStatus' : x[2]})
            for x in output ]
        return requests




