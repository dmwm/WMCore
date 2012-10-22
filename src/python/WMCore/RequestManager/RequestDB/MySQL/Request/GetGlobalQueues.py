#!/usr/bin/env python
"""
_Request.GetOverview_

API for getting a new request by its ID

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetGlobalQueues(DBFormatter):
    """
    _GetOverview_

    retrieve the details of all requests in the request table

    """
    sql = """SELECT distinct(prodmgr_id) AS global_queue
                FROM reqmgr_assigned_prodmgr
          """

    def execute(self, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """

        results = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        queues = []
        results = self.format(results)
        for result in results:
            queues.append(result[0])
        return queues
