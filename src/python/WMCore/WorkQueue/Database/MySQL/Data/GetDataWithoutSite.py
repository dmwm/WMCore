"""
_GetDataWithoutSite_

MySQL implementation of Data.GetDataWithoutSite
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class GetDataWithoutSite(DBFormatter):
    sql = """SELECT wb.id, wb.name, wt.dbs_url
                FROM wq_data wb
                LEFT OUTER JOIN wq_data_site_assoc dsa ON wb.id = dsa.data_id
                INNER JOIN wq_element we ON we.input_id = wb.id
                INNER JOIN wq_wmtask wt ON wt.id = we.wmtask_id
                WHERE dsa.data_id is NULL
          """

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        return self.formatDict(results)
