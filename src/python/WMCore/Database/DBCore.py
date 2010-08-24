#!/usr/bin/env python
"""
_Core_

Core Database APIs


"""
__revision__ = "$Id: DBCore.py,v 1.1 2008/04/10 19:45:10 evansde Exp $"
__version__ = "$Revision: 1.1 $"


from sqlalchemy.databases.mysql import MySQLDialect
from sqlalchemy.databases.sqlite import SQLiteDialect 
from sqlalchemy.databases.oracle import OracleDialect

class wmbsSQLFactory(object):
    """
    _wmbsSQLFactory_
    
    Factory to create WMBS instances. Could do something similar else where.

    DAVE: Factor into WMBSFactory module??? How general is this???
    
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
    
class wmSQL(object):    
    """
    Base class for doing SQL operations using a SQLAlchemy engine, or
    pre-exisitng connection.
    
    processData will take a (list of) sql statements and a (list of)
    bind variable dictionaries and run the statements on the DB. If
    necessary it will substitute binds into the sql (MySQL).
    
    TODO: 
        Add in some suitable exceptions in one or two places 
        Test the hell out of it
        Support executemany()
    """
    
    logger = None
    engine = None
    
    
    def __init__(self, logger, engine):
        self.logger = logger
        self.logger.info ("Instantiating base WM SQL object")
        self.engine = engine
    
    def executebinds(self, s=None, b=None, connection=None):
        """
        _executebinds_

        """
        try:
            if isinstance(self.engine.dialect, MySQLDialect) and b is not None:
                s, b = self.__substitute(s, b)
            self.logger.debug (s)
            self.logger.debug (b)
            if b == None: result = connection.execute(s)
            else: result = connection.execute(s, b)
            return result
        except Exception, e:
            self.logger.exception(e)
            self.logger.exception(s)
            self.logger.exception(b)
            raise e
    
    def __substitute(self, sql, binds):
        """
        Horrible hacky thing to handle MySQL's lack of bind variable support
        takes a single sql statment and its associated binds
        """
        newsql = sql
        if isinstance(self.engine.dialect, MySQLDialect) and binds is not None:
            for k, v in binds.items():
                newsql = newsql.replace(':%s' % k, "'%s'" % v)
            return newsql, None
            
    def processData(self, sqlstmt, binds = None, conn = None,
                    transaction = False):
        """
        set conn if you already have an active connection to reuse
        set transaction = True if you already have an active transaction
        
        TODO: convert resultset stuff into lists of tuples/dictionaries
        """
        if not conn: conn = self.engine.connect()
        result = []
        "Can take either a single statement or a list of staments and binds"
        if type(sqlstmt) == type("string") and binds is None:
            """
            Should never get executed - should be using binds!!
            """
            self.logger.warning('%s is not using binds!!' % sqlstmt)
            result.append(self.executebinds(sqlstmt, binds, connection=conn))            
        elif type(sqlstmt) == type("string") and isinstance(binds, dict):
            # single statement plus binds
            result.append(self.executebinds(sqlstmt, binds, connection=conn))
        elif type(sqlstmt) == type("string") and isinstance(binds, list):
            #Run single SQL statement for a list of binds
            if not transaction: trans = conn.begin()
            try:
                for b in binds:
                    result.append(self.executebinds(sqlstmt, b,
                                                    connection=conn))
                if not transaction: trans.commit()
            except Exception, e:
                if not transaction: trans.rollback()
                raise e
        elif isinstance(sqlstmt, list) and isinstance(binds, list) and len(binds) == len(sqlstmt):            
            # Run a list of SQL for a list of binds
            if not transaction: trans = conn.begin()
            try:
                for i, s in enumerate(sqlstmt):
                    b = binds[i]
                    result.append(self.executebinds(sqlstmt, b,
                                                    connection=conn))
                if not transaction: trans.commit()
            except Exception, e:
                if not transaction: trans.rollback()
                raise e
        else:
            self.logger.exception(
                "Nothing executed, problem with your arguments")
            self.logger.debug('sql is %s items long' % len(sqlstmt))
            self.logger.debug('binds are %s items long' % len(binds))
            self.logger.debug('are binds and sql same length? : %s' % (
                len(binds) == len(sqlstmt)),)
            self.logger.debug( sqlstmt, binds, conn, transaction)
            self.logger.debug( type(sqlstmt), type(binds),
                               type("string"), type({}),
                               type(conn), type(transaction))
            raise 'some clever exception type'
        if result:
            return result
        else: 
            return []
