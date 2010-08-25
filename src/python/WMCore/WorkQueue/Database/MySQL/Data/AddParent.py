"""
_New_

MySQL implementation of Block.AddParent
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class AddParent(DBFormatter):
    sql = """INSERT INTO wq_data_parentage (child, parent)
                 VALUES (SELECT id FROM wq_data WHERE name = :childName,
                         SELECT id FROM wq_data WHERE name = :parentName)
          """

    def execute(self, childName, parentName, conn = None, transaction = False):
        binds = {"childName": childName, "parentName": parentName}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
