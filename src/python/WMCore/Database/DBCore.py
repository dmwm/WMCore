#!/usr/bin/env python
"""
_Core_

Core Database APIs


"""
__revision__ = "$Id: DBCore.py,v 1.5 2008/05/12 11:58:08 swakef Exp $"
__version__ = "$Revision: 1.5 $"


from sqlalchemy.databases.mysql import MySQLDialect
    
class DBInterface(object):    
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
                s, b = self.substitute(s, b)
            self.logger.debug (s)
            self.logger.debug (b)
            if b == None: 
                result = connection.execute(s)
            else: 
                result = connection.execute(s, b)
            return result
        except Exception, e:
            self.logger.exception(e)
            self.logger.exception(s)
            self.logger.exception(b)
            raise e
    
    def substitute(self, sql, binds):
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
        if not conn: 
            connection = self.engine.connect()
        else: 
            connection = conn
            
        if not transaction: 
                trans = connection.begin()
            
        result = []
        # Can take either a single statement or a list of statements and binds
        if type(sqlstmt) == type("string") and binds is None:
            # Should never get executed - should be using binds!!
            self.logger.warning('%s is not using binds!!' % sqlstmt)
            result.append(self.executebinds(sqlstmt, binds, 
                                            connection=connection))            
        elif type(sqlstmt) == type("string") and isinstance(binds, dict):
            # single statement plus binds
            result.append(self.executebinds(sqlstmt, binds, 
                                            connection=connection))
        elif type(sqlstmt) == type("string") and isinstance(binds, list):
            #Run single SQL statement for a list of binds
            #if not transaction: 
            #    trans = connection.begin()
            #    print "start transaction"
            try:
                for b in binds:
                    result.append(self.executebinds(sqlstmt, b,
                                            connection=connection))
                #if not transaction: 
                #    trans.commit()
            except Exception, e:
                if not transaction: 
                    trans.rollback()
                raise e
        elif isinstance(sqlstmt, list) and isinstance(binds, list) \
                and len(binds) == len(sqlstmt):            
            # Run a list of SQL for a list of binds
            #if not transaction: 
             #   trans = connection.begin()
            try:
                for i, s in enumerate(sqlstmt):
                    b = binds[i]
                    result.append(self.executebinds(sqlstmt, b,
                                            connection=connection))
                #if not transaction:
                #    trans.commit()
            except Exception, e:
                if not transaction: 
                    trans.rollback()
                raise e
        else:
            self.logger.exception(
                "Nothing executed, problem with your arguments")
            self.logger.debug('sql is %s items long' % len(sqlstmt))
            self.logger.debug('binds are %s items long' % len(binds))
            self.logger.debug('are binds and sql same length? : %s' % (
                len(binds) == len(sqlstmt)),)
            self.logger.debug( sqlstmt, binds, connection, transaction)
            self.logger.debug( type(sqlstmt), type(binds),
                               type("string"), type({}),
                               type(connection), type(transaction))
            raise 'some clever exception type'
        
#        if not transaction:
#            #print "commiting transaction"
#            trans.commit()
#        if not conn: 
#            connection.close() # Return connection to the pool
        
        # change resultset to list
        #return result
        temp = []
        for row in result:
            for subrow in row.fetchall():
                #print "have %s" % subrow
                #temp.extend(subrow.values())
                temp.append(subrow.values())
            temp.extend(row.fetchall())
            #temp.append(row.fetchall())
#        print temp
        if not transaction:
            #print "commiting transaction"
            trans.commit()
        if not conn: 
            connection.close() # Return connection to the pool
            
        return temp
        
