#!/usr/bin/env python
"""
_Request.FindByTeam_

API for finding a new request by team

"""



from WMCore.Database.DBFormatter import DBFormatter

class FindByTeam(DBFormatter):
    """
    _Find_

    Find request ids based on status and team


    """
    def execute(self, teamId, reqStatus = None, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the teamId
        """
        if reqStatus != None:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id
             JOIN reqmgr_assignment assign
               ON req.request_id = assign.request_id
             WHERE stat.status_name = '%s' AND assign.team_id = %s
             """ % (reqStatus, teamId)
        else:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_assignment assign
               ON req.request_id = assign.request_id
             WHERE assign.team_id = %s
            """ % teamId
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))




