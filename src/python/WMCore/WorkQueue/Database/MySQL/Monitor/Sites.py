"""
WMCore/WorkQueue/Database/MySQL/Monitor/Sites.py

DAO object for WorkQueue

"""

__all__ = []
__revision__ = "$Id: Sites.py,v 1.1 2010/03/24 19:39:48 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter



class Sites(DBFormatter):
    sql = """SELECT id, name FROM wq_site"""
    
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)