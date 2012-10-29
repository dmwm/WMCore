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
        binds = {}
        if reqStatus != None:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id
             JOIN reqmgr_assignment assign
               ON req.request_id = assign.request_id
             WHERE stat.status_name = :req_status AND assign.team_id = :team_id
             """
            binds = {"req_status": reqStatus, "team_id": teamId}
        else:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_assignment assign
               ON req.request_id = assign.request_id
             WHERE assign.team_id = :team_id
            """
            binds = {"team_id": teamId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))
