"""
_GetWMSpecInfo_

MySQL implementation of WMSpec.GetWMSpecInfo
"""
__all__ = []
__revision__ = "$Id: GetWMSpecInfo.py,v 1.3 2010/05/12 19:13:55 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetWMSpecInfo(DBFormatter):
    sql = """SELECT ws.id, ws.name as wmspec_name, ws.url, ws.owner, 
                    wt.name as wmtask_name, wt.type as wmtask_type,
                    wt.dbs_url 
             FROM wq_wmspec ws 
             INNER JOIN wq_wmtask wt ON wt.wmspec_id = ws.id 
             WHERE wt.id = :wmtask_id"""
        
    def execute(self, wmTaskID, conn = None, transaction = False):
        binds = {'wmtask_id' : wmTaskID}
        
        results = self.dbi.processData(self.sql, binds, conn = conn,
                         transaction = transaction)
        return self.formatDict(results)[0]
        