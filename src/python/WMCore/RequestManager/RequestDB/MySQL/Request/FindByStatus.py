#!/usr/bin/env python
"""
_Request.Find_

API for finding a new request by status

"""
__revision__ = "$Id: FindByStatus.py,v 1.1 2010/07/01 19:12:39 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class FindByStatus(DBFormatter):
    """
    _Find_

    Find request ids based on status


    """
    def execute(self, reqStatus, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """
        self.sql = """
         SELECT req.request_name, req.request_id from reqmgr_request req
           JOIN reqmgr_request_status stat
             ON req.request_status = stat.status_id
           WHERE stat.status_name = "%s"
         """ % reqStatus




        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))
