class MySQLBase(object):
    def __init__(self, logger, dbinterface):
        self.logger = logger
        self.dbi = dbinterface
    
    def format(self, result):
        """
        Some standard formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                out.append(i)
        return out
    
    def getBinds(self):
        return {}
    
    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(), 
                         conn = conn, transaction = transaction)
        return self.format(result)