#!/usr/bin/env python
# pylint: disable-msg = R0903
"""
_SQLFactory_

A factory to create the appropriate WMBS database dialect object
for a given database engine. 

"""

__revision__ = "$Id: Factory.py,v 1.3 2008/05/02 14:15:18 metson Exp $"
__version__ = "$Revision: 1.3 $"

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
        #if isinstance(dia, OracleDialect):
        #    from WMCore.WMBS.Oracle import OracleDialect as WMBSOracle
        #    return WMBSOracle (self.logger, engine)
        #el
        if isinstance(dia, SQLiteDialect):
            from WMCore.WMBS.SQLite import SQLiteDialect as WMBSSQLite
            return WMBSSQLite (self.logger, engine)
        elif isinstance(dia, MySQLDialect):
            from WMCore.WMBS.MySQL import MySQLDialect as WMBSMySQL
            return WMBSMySQL (self.logger, engine)    
        else:
            raise TypeError, "unknown connection type"

#Make a similar factory for T0AST?