"""
WMCore/WorkQueue/Database/MySQL/Monitor/Sites.py

DAO object for WorkQueue

"""

__all__ = []




from WMCore.Database.DBFormatter import DBFormatter



class DataSiteMap(DBFormatter):
    sql = """SELECT d.id, d.name, s.name FROM wq_data_site_assoc
             INNER JOIN wq_data d ON (d.id = data_id)
             INNER JOIN wq_site s ON (s.id = site_id)
             ORDER BY d.id"""
    
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)