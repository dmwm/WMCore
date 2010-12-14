#!/usr/bin/env python
"""
_AddLocation_

MySQL implementation of DBSBufferFiles.AddLocation
"""

import copy

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.Transaction import Transaction

class AddLocation(DBFormatter):
    existsSQL = """SELECT se_name, id FROM dbsbuffer_location
                     WHERE se_name = :location FOR UPDATE"""

    sql = """INSERT IGNORE INTO dbsbuffer_location (se_name)
               VALUES (:location)"""
    
    def execute(self, siteName, conn = None, transaction = False):
        """
        _execute_

        Determine if the sites already exist in the dbsbuffer_location and
        attempt to lock the table using a "FOR UPDATE" parameters on the
        select statement.  Insert any sites that do not already exist in the
        location table and return the IDs of all sites that were passed to this
        function.

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
            self.dbi.processData(self.sql, binds, conn = myTransaction.conn, 
                                 transaction = True)
            results = self.dbi.processData(self.existsSQL, binds,
                                           conn = myTransaction.conn,
                                           transaction = True)

            results = self.format(results)
            for result in results:
                nameMap[result[0]] = int(result[1])
                
        myTransaction.commit()
        return nameMap
