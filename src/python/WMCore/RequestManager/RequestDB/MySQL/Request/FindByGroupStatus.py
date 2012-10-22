#!/usr/bin/env python
"""
_Request.Find_

API for finding a new request by status

"""



from WMCore.Database.DBFormatter import DBFormatter

class FindByGroupStatus(DBFormatter):
    """
    _Find_

    Find request ids based on status and group


    """
    def execute(self, groupId, reqStatus = None, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the groupId
        """
        binds = {}
        if reqStatus != None:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id
             JOIN reqmgr_group_association assoc
               ON req.requestor_group_id = assoc.association_id
             WHERE stat.status_name = ":req_status" AND assoc.group_id = :group_id"""
            binds = {"req_status": reqStatus, "group_id": groupId}
        else:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_group_association assoc
               ON req.requestor_group_id = assoc.association_id
             WHERE assoc.group_id = :group_id"""
            binds = {"group_id": groupId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))
