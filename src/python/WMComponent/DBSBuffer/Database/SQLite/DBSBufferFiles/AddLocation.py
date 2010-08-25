"""
SQLite implementation of AddLocation,
Adds a Location to database if it doesn't exists already
"""

__revision__ = "$Id: AddLocation.py,v 1.1 2009/05/14 16:21:50 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddLocation import AddLocation as MySQLAddLocation

class AddLocation(MySQLAddLocation):

    sql = """INSERT INTO dbsbuffer_location(se_name) VALUES (:location)""" 


    def execute(self, location, conn = None, transaction = None):
        binds = self.getBinds(location)
	try:
	        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        except Exception, ex:
            if ex.__str__().find("unique") != -1 :
                return
            else:
                raise ex
        return
