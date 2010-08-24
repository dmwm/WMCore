"""
A basic action is a thing that will run a SQL statement

A more complex one would be something that ran multiple SQL 
objects to produce a single output.

This class should generally not be used unless you know what 
you are doing. DAOFactory is the preferred method for 
accessing WM DAO classes.

"""
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import SQLiteDialect
from WMCore.Database.Dialects import OracleDialect
from WMCore.DAOFactory import DAOFactory

class BaseAction(object):
    name = "BaseAction"
    def __init__(self, package='WMCore', logger=None):
        self.package = package
        self.logger = logger
        self.logger.debug("Instantiating %s Action object" % self.name)
       
    def execute(self, dbinterface = None):
        daofactory = DAOFactory(package=self.package, logger=self.logger, dbinterface=dbinterface)
        action = daofactory(classname=self.name)
        try:
            return action.execute()
        except Exception, e:
            self.logger.exception(e)
            return False

class BoundAction(BaseAction):
    """
    Subclass of BaseAction that takes kwargs to the execute method
    """
    
    def execute(self, **kwargs):
        if not 'dbinterface' in kwargs.keys():
            raise ValueError, "You must pass a dbinterface to a BoundAction's execute method"
        daofactory = DAOFactory(package=self.package, logger=self.logger, dbinterface=dbinterface)
        action = daofactory(classname=self.name)
        try:
            return action.execute(kwargs)
        except Exception, e:
            self.logger.exception(e)
            return False