#!/usr/bin/env python
"""
_Request.FindByName_

API for finding a new request by name

"""



from WMCore.Database.DBFormatter import DBFormatter

class FindByName(DBFormatter):
    """
    _Find_

    Find request ids based on name


    """
    def execute(self, reqName, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """
        self.sql = """
         SELECT req.request_id from reqmgr_request req
           WHERE req.request_name = '%s'
         """ % reqName


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return self.format(result)[0][0]
