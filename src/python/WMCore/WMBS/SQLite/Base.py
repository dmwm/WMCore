from WMCore.WMBS.MySQL.Base import MySQLBase

class SQLiteBase(MySQLBase):
    
    def format(self, result):
        """
        Some standard formatting making allowances for the difference with MySQL
        To be deprecated in preference of WMCore.Database.DBFormatter
        """
        out = []
        for r in result:
            for i in r.fetchall():
                row = []
                for j in i:
                    row.append(j)
                out.append(row)
        return out
        
    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.format(result)