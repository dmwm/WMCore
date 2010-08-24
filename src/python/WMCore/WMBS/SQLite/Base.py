from WMCore.WMBS.MySQL.Base import MySQLBase

class SQLiteBase(MySQLBase):
    
    def format(self, result):
        """
        Some standard formatting making allowances for the difference with MySQL
        """
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i)
        return out
    
    def getBinds(self):
        """
        Return the appropriately formatted binds for the sql
        """
        return {}
        
    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.format(result)