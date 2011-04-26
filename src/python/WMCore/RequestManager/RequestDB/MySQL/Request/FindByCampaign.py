#!/usr/bin/env python
"""
_Request.FindByCampaign_

API for finding a new request by campaign

"""



from WMCore.Database.DBFormatter import DBFormatter

class FindByCampaign(DBFormatter):
    """
    _Find_

    Find request ids based on status and campaign


    """
    def execute(self, campaignId, reqStatus=None, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the campaignId
        """
        if reqStatus != None:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_request_status stat
               ON req.request_status = stat.status_id
             JOIN reqmgr_campaign_assoc assoc
               ON req.request_id = assoc.request_id
             WHERE stat.status_name = '%s' AND assoc.campaign_id = %s
             """ % (reqStatus, campaignId)
        else:
            self.sql = """
           SELECT req.request_name, req.request_id FROM reqmgr_request req
             JOIN reqmgr_campaign_assoc assoc
               ON req.request_id = assoc.request_id
             WHERE assoc.campaign_id = %s
            """ % campaignId
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))




