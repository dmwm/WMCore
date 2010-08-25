#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the 
FeederManage. It should be split up into DAO objects
"""

import time

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the MySQL backend for the FeederManager
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        

    # FIXME: these are all single inserts
    # find a way to do this in bulk.
    # we can do this if we enable a thread slave
    # retrieve messages from the queue in bulk.   


    def addFeeder(self, feederType, feederState):
        """
        Adds a managed feeder
        """
        sqlStr = """
INSERT INTO managed_feeders(feeder_type, feeder_state, insert_time)
VALUES (:type, :state, :time) 
"""
        self.execute(sqlStr, {'type' : feederType, 'state' : feederState, \
                              'time' : int(time.time())})



    def checkFeeder(self, feederType):
        """
        Checks if a given feeder type is already instantiated
        """
        sqlStr = """
SELECT COUNT(*) FROM managed_feeders WHERE feeder_type = :type"""
        result = self.execute(sqlStr, {'type':feederType})
        return self.formatOne(result)[0] != 0


    
    def getFeederId(self, feederType):
        """
        Gets the ID for a given feeder
        """
        sqlStr = """
SELECT id from managed_feeders WHERE feeder_type = :type"""
        result = self.execute(sqlStr, {"type" : feederType})
        return self.formatOne(result)[0]


    def checkFileset(self, fileset, feederType):
        """
        Check if a given fileset is already managed 
        """

        sqlStr = """
SELECT insert_time FROM managed_filesets \ 
WHERE feeder = :type and fileset = :fileset
"""
        result = self.execute(sqlStr, \
  {"type" : feederType, 'fileset' : fileset }) 
        #return self.formatOne(result)[0]
        return self.formatDict(result)


    def addFilesetToManage(self, fileset, feederType):
        """
        Adds a fileset for beeing managed by feeder
        """

        sqlStr = """
INSERT INTO managed_filesets(fileset, feeder, insert_time)
VALUES (:id, :type, :time)
"""
        self.execute(sqlStr, {'id' : fileset, 'type' : feederType, \
                              'time' : int(time.time())})



    def removeManagedFilesets(self, filesetId, feederType):
        """
        Removes a filesets from being managed
        """
        sqlStr = """DELETE FROM managed_filesets
                    WHERE fileset = :fileset
                    AND feeder = :feederType
                    """
        self.execute(sqlStr, {'fileset' : filesetId, \
                              'feederType' : feederType})



    def getManagedFilesets(self, feederType):
        """
        Returns all fileset patterns that are currently being
        managed by the feeder feederType
        """

        sqlStr = """
SELECT id, name from wmbs_fileset 
WHERE EXISTS (SELECT 1 FROM managed_filesets WHERE managed_filesets.fileset = wmbs_fileset.id and managed_filesets.feeder = :feederType)
"""

        result = self.execute(sqlStr, {'feederType' : feederType})
        return self.formatDict(result)

    def getAllManagedFilesets(self):
        """
        Returns all fileset patterns that are currently being
        managed 
        """

        sqlStr = """
SELECT id, name from wmbs_fileset 
WHERE EXISTS (SELECT 1 FROM managed_filesets WHERE managed_filesets.fileset = wmbs_fileset.id )

"""

        result = self.execute(sqlStr, {})
        return self.formatDict(result)




#     def getUnManagedFilesets(self):
#        """
#        Returns all filesets that do not have been managed
#        """
#        sqlStr = """
#SELECT wmbs_fileset.id, wmbs_fileset.name FROM wmbs_fileset 
#WHERE NOT EXISTS (SELECT 1 FROM managed_filesets \
#WHERE managed_filesets.fileset = wmbs_fileset.id)
#"""
#        result = self.execute(sqlStr, {})
#        return self.formatDict(result)


    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 
