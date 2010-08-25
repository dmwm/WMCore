"""
_InsertComponent_

MySQL implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: InsertComponent.py,v 1.1 2010/06/21 21:19:10 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class InsertComponent(DBFormatter):
    
    deleteWorkerSql = """DELETE FROM wm_workers 
                            WHERE component_id = (SELECT id FROM wm_components 
                                   WHERE name = :name)
                       """
                                   
    deleteSql = """DELETE FROM wm_components 
                       WHERE name = :name"""
                                                       
    sql = """INSERT INTO wm_components (name, pid, update_threshold) 
             VALUES (:name, :pid, :update_threshold)"""

    def execute(self, name, pid, update_threshold = 6000,
                conn = None, transaction = False):
        deleteBinds = {"name": name}
        
        self.dbi.processData(self.deleteWorkerSql, deleteBinds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.deleteSql, deleteBinds, conn = conn,
                             transaction = transaction)
        binds = {"name": name, "pid": pid, "update_threshold": update_threshold}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return

