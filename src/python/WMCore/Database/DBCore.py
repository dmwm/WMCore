#!/usr/bin/env python
"""
_Core_

Core Database APIs


"""
__revision__ = "$Id: DBCore.py,v 1.15 2008/08/21 10:27:49 metson Exp $"
__version__ = "$Revision: 1.15 $"

from copy import copy   
from WMCore.DataStructs.WMObject import WMObject

class DBInterface(WMObject):    
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
        self.logger.info ("Instantiating base WM DBInterface")
        self.engine = engine
    
    def buildbinds(self, sequence, thename, therest = [{}]):
        """
        Build a list of binds. Can be used recursively, e.g.:
        buildbinds(file, 'file', buildbinds(sename, 'location'), {'lumi':123})
        TODO: replace with an appropriate map function 
        """
        binds = []
        for r in sequence:
            for i in self.makelist(therest):
                    thebind = copy(i)
                    thebind[thename] = r
                    binds.append(thebind)
        return binds

    def executebinds(self, s=None, b=None, connection=None):
        """
        _executebinds_
        """
        try:
            self.logger.debug ('DBInterface.executebinds - sql : %s' % s)
            self.logger.debug ('DBInterface.executebinds - binds : %s' % b)
            if b == None: 
                result = connection.execute(s)
            else: 
                result = connection.execute(s, b)
            return result
        except Exception, e:
            self.logger.exception('DBInterface.executebinds - exception type: %s' % type(e))
            self.logger.exception('DBInterface.executebinds - connection type: %s' % type(connection))
            self.logger.exception('DBInterface.executebinds - connection %s' % connection)
            self.logger.exception('DBInterface.executebinds - sql : %s' % s)
            self.logger.exception('DBInterface.executebinds - binds : %s' % b)
            self.logger.debug(e)
            raise e
        

    def executemanybinds(self, s=None, b=None, connection=None):
        """
        _executemanybinds_
        b is a list of dictionaries for the binds, e.g.:
        
        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
        {'bind1':'value1b', 'bind2': 'value2b'} ]
        
        see: http://www.gingerandjohn.com/archives/2004/02/26/cx_oracle-executemany-example/
        """
        try:
            self.logger.debug ('DBInterface.executebinds - sql : %s' % s)
            self.logger.debug ('DBInterface.executebinds - binds : %s' % b)
            #Maybe need to get the cursor???
            result = connection.execute(s, b)
            return result
        except Exception, e:
            self.logger.exception('DBInterface.executebinds - exception type: %s' % type(e))
            self.logger.exception('DBInterface.executebinds - connection type: %s' % type(connection))
            self.logger.exception('DBInterface.executebinds - connection %s' % connection)
            self.logger.exception('DBInterface.executebinds - sql : %s' % s)
            self.logger.exception('DBInterface.executebinds - binds : %s' % b)
            self.logger.debug(e)
            raise e
    
    def connection(self):
        """
        Return a connection to the engine (from the connection pool)
        """
        return self.engine.connect()
    
    def processData(self, sqlstmt, binds = None, conn = None,
                    transaction = False):
        """
        set conn if you already have an active connection to reuse
        set transaction = True if you already have an active transaction        
        TODO: Make this code cleaner
        """
        if not conn: 
            connection = self.connection()
        else: 
            connection = conn
        result = []
        
        # Can take either a single statement or a list of statements and binds
        if type(sqlstmt) == type("string") and binds is None:
            # Should never get executed - should be using binds!!
            self.logger.warning('''The following statement is not using binds!! \n 
                        %s''' % sqlstmt)
            result.append(self.executebinds(sqlstmt, 
                                            connection=connection))  
        elif isinstance(sqlstmt, list) and binds is None:
            # Should only be run by create statements
            if not transaction: 
                trans = connection.begin()
            try:
                for i in sqlstmt:
                    self.logger.warning('''The following statement is not using binds!! \n 
                        %s''' % i)
                    result.append(self.executebinds(i, connection=connection))
                if not transaction: 
                    trans.commit()
            except Exception, e:
                if not transaction: 
                    trans.rollback()
                raise e 
            
        elif type(sqlstmt) == type("string") and isinstance(binds, dict):
            # single statement plus binds
            result.append(self.executebinds(sqlstmt, binds, 
                                            connection=connection))
            
        elif type(sqlstmt) == type("string") and isinstance(binds, list):
            #Run single SQL statement for a list of binds
            if not transaction: 
                trans = connection.begin()
            try:
                result.append(self.executemanybinds(sqlstmt, binds,
                                            connection=connection))
                if not transaction: 
                    trans.commit()
            except Exception, e:
                if not transaction: 
                    trans.rollback()
                raise e
            
        elif isinstance(sqlstmt, list) and isinstance(binds, list) \
                and len(binds) == len(sqlstmt):            
            # Run a list of SQL for a list of binds
            if not transaction: 
                trans = connection.begin()
            try:
                for i, s in enumerate(sqlstmt):
                    b = binds[i]
                    result.append(self.executebinds(s, b,
                                            connection=connection))
                if not transaction: 
                    trans.commit()
            except Exception, e:
                if not transaction: 
                    trans.rollback()
                raise e 
            
        else:
            self.logger.exception(
                "DBInterface.processData Nothing executed, problem with your arguments")
            self.logger.exception(
                "DBInterface.processData SQL = %s" % sqlstmt)
            self.logger.debug('DBInterface.processData  sql is %s items long' % len(sqlstmt))
            self.logger.debug('DBInterface.processData  binds are %s items long' % len(binds))
            assert_value = False
            if len(binds) == len(sqlstmt):
                assert_value =True 
            self.logger.debug('DBInterface.processData are binds and sql same length? : %s' % (assert_value))
            self.logger.debug( sqlstmt, binds, connection, transaction)
            self.logger.debug( type(sqlstmt), type(binds),
                               type("string"), type({}),
                               type(connection), type(transaction))
            raise Exception, """DBInterface.processData Nothing executed, problem with your arguments 
Probably mismatched sizes for sql (%i) and binds (%i)""" % (len(sqlstmt), len(binds))
        if not conn: 
            connection.close() # Return connection to the pool
        return result
        
