#!/usr/bin/env python
"""
_Request.FindByProdMgr_

API for finding a new request by prodmgr association

"""



from WMCore.Database.DBFormatter import DBFormatter

class FindByProdMgr(DBFormatter):
    """
    _Find_

    Find request ids based on ProdMgr association


    """
    def execute(self, prodMgr, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the associated prodmgr
        """
        self.sql = """
        SELECT req.request_name, req.request_id, stat.status_name
           FROM reqmgr_request req
            JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id

            JOIN reqmgr_assigned_prodmgr assoc
               ON req.request_id = assoc.request_id
            WHERE assoc.prodmgr_id = '%s'
        """ % prodMgr


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








