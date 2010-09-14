#!/usr/bin/env python
"""
_Request.GetOverview_

API for getting a new request by its ID

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetOverview(DBFormatter):
    """
    _GetOverview_

    retrieve the details of all requests in the request table

    """
    sql = """SELECT r.request_id, r.request_name,
                    rt.type_name AS type,
                    rs.status_name AS status,
                    rp.prodmgr_id AS global_queue
                FROM reqmgr_request r
                INNER JOIN reqmgr_request_type rt
                        ON rt.type_id = r.request_type
                INNER JOIN reqmgr_request_status rs
                        ON rs.status_id = r.request_status
                LEFT OUTER JOIN reqmgr_assigned_prodmgr rp
                        ON rp.request_id = r.request_id
                ORDER BY rs.status_id, r.request_id
          """

    def execute(self, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """

        results = self.dbi.processData(self.sql,
                            conn = conn, transaction = trans)
        return self.formatDict(results)
