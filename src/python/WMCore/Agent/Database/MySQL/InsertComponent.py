"""
_InsertComponent_

MySQL implementation of Block.New
"""

__all__ = []

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

    def execute(self, name, pid, update_threshold,
                conn=None, transaction=False):
        deleteBinds = {"name": name}

        self.dbi.processData(self.deleteWorkerSql, deleteBinds, conn=conn,
                             transaction=transaction)
        self.dbi.processData(self.deleteSql, deleteBinds, conn=conn,
                             transaction=transaction)
        binds = {"name": name, "pid": pid, "update_threshold": update_threshold}
        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)
        return
