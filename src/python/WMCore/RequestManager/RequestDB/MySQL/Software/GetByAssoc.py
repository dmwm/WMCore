#!/usr/bin/env python
"""
_GetByAssoc_

Get software dependencies by association to a request

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetByAssoc(DBFormatter):
    """
    _GetByAssoc_

    retrieve the software details given an association to a request

    """
    def execute(self, requestId, conn = None, trans = False):
        """
        _execute_

        get the software dependencies for a request given the request id

        """
        self.sql = """
        select reqmgr_software.software_id,reqmgr_software.software_name
         from reqmgr_software LEFT OUTER JOIN reqmgr_software_dependency
     ON reqmgr_software.software_id = reqmgr_software_dependency.software_id
          where reqmgr_software_dependency.request_id = :request_id"""
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))
