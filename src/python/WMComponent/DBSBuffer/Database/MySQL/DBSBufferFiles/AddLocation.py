"""
MySQL implementation of AddLocation, Adds a Location to database if it doesn't exists already
"""
from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class AddLocation(DBFormatter):

    sql = """insert dbsbuffer_location(se_name) values (:location)""" 

    def getBinds(self, location = None):

        if type(location) == type('string'):
            return self.dbi.buildbinds(self.dbi.makelist(location), 'location')

        elif isinstance(location, (list, Set, set)):
            binds = []
            for l in location:
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(file), 'lfn',
                   self.dbi.buildbinds(self.dbi.makelist(l), 'location')))
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
            if ex.__str__().find("Duplicate entry") != -1 :
                return
            else:
                raise ex
        return

