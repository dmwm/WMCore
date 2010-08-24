from sqlalchemy.databases.mysql import MySQLDialect
from sqlalchemy.databases.sqlite import SQLiteDialect 
from sqlalchemy.databases.oracle import OracleDialect

class SQLFactory(object):
    """
    _wmbsSQLFactory_
    
    Factory to create WMBS database instances. Could do something similar else where.
    
    """
    logger = ""
    def __init__(self, logger):
        self.logger = logger
        self.logger.info("Instantiating WMBS SQL Factory")
        
    def connect (self, engine):
        """
        _connect_
        
        """
        dia = engine.dialect
        if isinstance(dia, OracleDialect):
            from WMBSOracle import wmbsOracle
            return wmbsOracle (self.logger, engine)
        elif isinstance(dia, SQLiteDialect):
            from WMBSSQLite import wmbsSQLite
            return wmbsSQLite (self.logger, engine)
        elif isinstance(dia, MySQLDialect):
            from WMBSMySQL import wmbsMySQL
            return wmbsMySQL (self.logger, engine)    
        else:
            "Could return the wmbsSQL object and hope that the connection"
            "can handle it but that might be a bit dangerous!"
            raise "unknown connection type"

#Make a similar factory for T0AST?