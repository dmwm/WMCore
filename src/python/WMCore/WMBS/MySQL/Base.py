"""
Class to define the standardised formatting of MySQL results.

To be deprecated in preference of WMCore.Database.DBFormatter
"""
import datetime
import time
from WMCore.Database.DBFormatter import DBFormatter

class MySQLBase(DBFormatter):
    def __init__(self, logger, dbinterface):
        self.logger = logger
        self.dbi = dbinterface