"""
WMCore/WorkQueue/Database/MySQL/Monitor/TopLevelJobsByRequest.py
DAO object for WorkQueue

"""


from WMCore.Database.DBFormatter import DBFormatter

class TopLevelJobsByRequest(DBFormatter):
    
    sql = """SELECT request_name, CAST(SUM(num_jobs) AS UNSIGNED) as total_jobs  
             FROM wq_element GROUP BY request_name """
    
            
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        return self.formatDict(results)
    
