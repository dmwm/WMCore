"""
A basic action is a thing that will run a SQL statement

A more complex one would be something that ran multiple SQL 
objects to produce a single output.
"""
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import SQLiteDialect
from WMCore.Database.Dialects import OracleDialect

class BaseAction(object):
    name = "BaseAction"
    def __init__(self, logger):
        self.logger = logger
        self.logger.debug("Instantiating %s Action object" % self.name)
        self.dialects = {"oracle" : OracleDialect,
                    "mysql" : MySQLDialect,
                    "sqlite" : SQLiteDialect}
# Fin       