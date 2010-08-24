#!/usr/bin/python

"""
_DBCreator_

Base class for formatters that create tables.

"""

__revision__ = "$Id: DBCreator.py,v 1.1 2008/08/16 05:08:56 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"



from WMCore.Database.DBFormatter import DBFormatter


class DBCreator(DBFormatter):
    
    """
    _DBCreator_
    
    Generic class for creating database tables.
    
    """
    
    def __init__(self, logger, dbinterface):
        """
        _init_
        
        pass parameters to parent class
        """
        DBFormatter.__init__(self, logger, dbinterface)
        self.create = {}
        self.constraints = {}
    
            
    def execute(self, conn = None, transaction = False):     
        """
        _execute_
        
        Generic method to create tables and constraints by execute
        sql statements in the create, and constraints dictionaries.
        """
        
        for i in self.create.keys():
            self.logger.debug( i )
            try:
                self.dbi.processData(self.create[i], 
                                     conn = conn, 
                                     transaction = transaction)
            except Exception, e:
                self.logger.debug( e )
            
            keys = self.constraints.keys()
            self.logger.debug( keys )
        for i in self.constraints.keys():
            self.logger.debug( i )
            try:
                self.dbi.processData(self.constraints[i], 
                                 conn = conn, 
                                 transaction = transaction)
            except Exception, e:
                self.logger.debug( e )
            
        return True
   