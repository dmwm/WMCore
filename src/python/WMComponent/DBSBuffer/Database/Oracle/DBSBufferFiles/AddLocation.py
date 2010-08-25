#!/usr/bin/env python

"""
Oracle implementation of AddLocation, Adds a Location to database if it doesn't exists already
"""


#Someone should check this!
#It's been modified for Oracle by taking out the lfn marker

__revision__ = "$Id: AddLocation.py,v 1.1 2009/05/15 16:47:40 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from sets import Set
from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddLocation import AddLocation as MySQLAddLocation

class AddLocation(MySQLAddLocation):

    sql = """insert into dbsbuffer_location(se_name) values (:location)""" 

    def getBinds(self, location = None):

        if type(location) == type('string'):
            return self.dbi.buildbinds(self.dbi.makelist(location), 'location')

        elif isinstance(location, (list, Set, set)):
            binds = []
            for l in location:
                #This line has been changed
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(l), 'location'))
            return binds
        else:
            raise Exception, "Type of location argument is not allowed: %s" \
                                % type(location)

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

