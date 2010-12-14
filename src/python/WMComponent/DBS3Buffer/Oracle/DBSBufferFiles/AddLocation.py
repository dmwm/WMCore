#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""




import copy

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.Transaction import Transaction

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddLocation import AddLocation as \
     MySQLAddLocation

class AddLocation(MySQLAddLocation):
    existsSQL = """SELECT se_name, id FROM dbsbuffer_location
                         WHERE se_name = :location"""

    sql = """INSERT INTO dbsbuffer_location (se_name) 
               SELECT :location AS se_name FROM DUAL WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = :location)"""
    
    def execute(self, siteName, conn = None, transaction = False):
        """
        _execute_

        Determine if the sites already exist in the dbsbuffer_location and
        insert any sites that do not already exist in the location table and
        return the IDs of all sites that were passed to this function.

        The sites will be returned as a dictionary where each key is the site
        name and the value is the site ID.

        This DAO will create it's own transaction and execute all SQL in that.
        This is done so that other transactions can pickup news sites and to
        avoid deadlocks.
        """
        mySites = copy.deepcopy(siteName)
        nameMap = {}
        
        if type(mySites) == str:
            mySites = [mySites]

        myTransaction = Transaction(self.dbi)
        myTransaction.begin()

        binds = []
        for location in mySites:
            binds.append({"location": location})
            
        results = self.dbi.processData(self.existsSQL, binds,
                                       conn = myTransaction.conn, 
                                       transaction = True)
        results = self.format(results)
        for result in results:
            nameMap[result[0]] = int(result[1])
            mySites.remove(result[0])

        binds = []
        for location in mySites:
            binds.append({"location": location})

        if len(binds) > 0:
            try:
                self.dbi.processData(self.sql, binds, conn = myTransaction.conn, 
                                     transaction = True)
            except Exception, ex:
                if "orig" in dir(ex) and type(ex.orig) != tuple:
                    if str(ex.orig).find("ORA-00001: unique constraint") != -1 and \
                       str(ex.orig).find("DBSBUFFER_LOCATION_UNIQUE") != -1:
                        return
                raise ex
            
            results = self.dbi.processData(self.existsSQL, binds,
                                           conn = myTransaction.conn,
                                           transaction = True)

            results = self.format(results)
            for result in results:
                nameMap[result[0]] = int(result[1])
                
        myTransaction.commit()
        return nameMap
