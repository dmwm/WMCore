#!/usr/bin/env python
"""
_DBSBufferFile_

A simple object representing a file in DBSBuffer.
"""

__revision__ = "$Id: DBSBufferFile.py,v 1.6 2009/10/22 15:24:01 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from sets import Set
import time
import threading
import logging

from WMCore.DataStructs.File import File as WMFile
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.Transaction import Transaction

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.WMBSBase import WMBSBase

class DBSBufferFile(WMBSBase, WMFile):
    def __init__(self, lfn = None, id = -1, size = None,
                 events = None, cksum = None, parents = None, locations = None,
                 status = "NOTUPLOADED"):
        WMBSBase.__init__(self)
        WMFile.__init__(self, lfn = lfn, size = size, events = events, 
                        cksum = cksum, parents = parents)
        self.setdefault("status", status)
        self.setdefault("id", id)
        self.setdefault("location", Set())

        # Parameters for the algorithm
        self.setdefault("appName", None)
        self.setdefault("appVer", None)
        self.setdefault("appFam", None)
        self.setdefault("psetHash", None)
        self.setdefault("configContent", None)
        self.setdefault("datasetPath", None)
        
        if locations == None:
            self.setdefault("newlocations", Set())
        else:
            self.setdefault("newlocations", self.makeset(locations))

        # The WMBS base class creates a DAO factory for WMBS, we'll need to
        # overwrite that so we can use the factory for DBSBuffer objects.
        self.daoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        return

    def exists(self):
        """
        _exists_

        Determine whether or not a file with this LFN exists inside the
        database.  Return the file's ID if it exists, False otherwise.
        """
        action = self.daoFactory(classname = "DBSBufferFiles.Exists")
        return action.execute(lfn = self["lfn"], conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        
    def getStatus(self):
        """
        _getStatus_

        Retrieve the status of the file.  This can be one of the following:
          UPLOADED
          NOTUPLOADED
	"""
        return self["status"]

    def getLocations(self):
        """
        _getLocations_

        Retrieve a list of locations where this file is stored.
	"""
        return list(self["locations"])

    def getRuns(self):
        """
        _getRuns_

        Retrieve a list of WMCore.DataStructs.Run objects that represent which
        run/lumi sections this file contains.
	"""
        return list(self["runs"])
                                    
    def load(self, parentage = 0):
        """
        _load_

        The the file and all it's metadata from the database.  Either the LFN
        or the file's ID must be specified before this is called.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] != -1:
            action = self.daoFactory(classname = "DBSBufferFiles.GetByID")
            result = action.execute(self["id"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daoFactory(classname = "DBSBufferFiles.GetByLFN")
            result = action.execute(self["lfn"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        self.update(result)

        action = self.daoFactory(classname = "DBSBufferFiles.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daoFactory(classname = "DBSBufferFiles.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn = self.getDBConn(),
                                           transaction = self.existingTransaction()) 

        self["newlocations"].clear()
        self["parents"].clear()
        
        if parentage > 0:
            action = self.daoFactory(classname = "DBSBufferFiles.GetParents")
            lfns = action.execute(self["lfn"], conn = self.getDBConn(),
                                  transaction = self.existingTransaction())
            for lfn in lfns:
                parentFile = DBSBufferFile(lfn = lfn)
                parentFile.load(parentage = parentage - 1)
                self["parents"].add(parentFile)

        self.commitTransaction(existingTransaction)
        return

    def insertDatasetAlgo(self):
        """
        _insertDatasetAlgo_

        Insert the dataset and algorithm for this file into the DBS Buffer.
        The dataset, algorithm and association between the two will be inserted
        as a seperate transaction to avoid a race between two processes trying
        to insert the same dataset/algo.  
        """
        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        newDatasetAction = self.daoFactory(classname = "NewDataset")
        assocAction = self.daoFactory(classname = "AlgoDatasetAssoc")      

        myThread = threading.currentThread()
        localTransaction = Transaction(myThread.dbi)
        localTransaction.begin()

        newAlgoAction.execute(appName = self["appName"], appVer = self["appVer"],
                              appFam = self["appFam"], psetHash = self["psetHash"],
                              configContent = self["configContent"],
                              conn = localTransaction.conn,
                              transaction = True)

        newDatasetAction.execute(datasetPath = self["datasetPath"],
                                 conn = localTransaction.conn,
                                 transaction = True)

        assocID = assocAction.execute(appName = self["appName"], appVer = self["appVer"],
                                      appFam = self["appFam"], psetHash = self["psetHash"],
                                      datasetPath = self["datasetPath"],
                                      conn = localTransaction.conn,
                                      transaction = True)
        localTransaction.commit()
        return assocID
    
    def create(self):
        """
        _create_

        Insert this file and all it's metadata into the DBS Buffer.
        """
        existingTransaction = self.beginTransaction()

        if self.exists() != False:
            self.load()
            return

        assocID = self.insertDatasetAlgo()

        addAction = self.daoFactory(classname = "DBSBufferFiles.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum= self["cksum"],
                          datasetAlgo = assocID, status = self["status"],
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        if len(self["runs"]) > 0:        
            lumiAction = self.daoFactory(classname="DBSBufferFiles.AddRunLumi")
            lumiAction.execute(file = self["lfn"], runs = self["runs"],
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

        self["id"] = self.exists()
        self.updateLocations()
        self.commitTransaction(existingTransaction)
        return
    
    def delete(self):
        """
        _delete_
        
        Remove a file from the DSBuffer database.
        """
        action = self.daoFactory(classname = "DBSBufferFiles.Delete")
        action.execute(file = self["lfn"], conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        return
        
    def addChildren(self, lfns):
        """
        _addChildren_
        
        Set one or more lfns as the child of this file.
        """
        if type(lfns) != list:
            lfns = [lfns]
            
        existingTransaction = self.beginTransaction()

        if not self["id"] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self["lfn"]

        action = self.daoFactory(classname = "DBSBufferFiles.HeritageLFNChild")
        action.execute(childLFNs = lfns, parentID = self["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
        
    def addParents(self, parentLFNs):
        """
        _addParents_

        Associate this file with it's parents.  If the parents do not exist in
        the buffer then bogus place holder files will be created so that the
        parentage information can be tracked and correctly inserted into DBS.

        Note that the bogus parents are created in a seperate transaction.  This
        helps avoid a race between two processes trying to insert the same
        parent files into the buffer.
        """
        newAlgoAction = self.daoFactory(classname = "NewAlgo")
        newDatasetAction = self.daoFactory(classname = "NewDataset")
        assocAction = self.daoFactory(classname = "AlgoDatasetAssoc")      
        existsAction = self.daoFactory(classname = "DBSBufferFiles.Exists")

        toBeCreated = []
        for parentLFN in parentLFNs:
            self["parents"].add(DBSBufferFile(lfn = parentLFN))
            if not existsAction.execute(lfn = parentLFN,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction()):
                toBeCreated.append(parentLFN)

        if len(toBeCreated) > 0:
            myThread = threading.currentThread()
            localTransaction = Transaction(myThread.dbi)
            localTransaction.begin()            
            newAlgoAction.execute(appName = "cmsRun", appVer = "UNKNOWN",
                                  appFam = "UNKNOWN", psetHash = "GIBBERISH",
                                  configContent = "MOREBIGGERISH",
                                  conn = localTransaction.conn,
                                  transaction = True)

            newDatasetAction.execute(datasetPath = "/bogus/dataset/path",
                                     conn = localTransaction.conn,
                                     transaction = True)

            assocID = assocAction.execute(appName = "cmsRun", appVer = "UNKNOWN",
                                          appFam = "UNKNOWN", psetHash = "GIBBERISH",
                                          datasetPath = "/bogus/dataset/path",
                                          conn = localTransaction.conn,
                                          transaction = True)

            action = self.daoFactory(classname = "DBSBufferFiles.AddIgnore")
            action.execute(lfns = toBeCreated, datasetAlgo = assocID,
                           status = "AlreadyInDBS",
                           conn = localTransaction.conn,
                           transaction = True)

            localTransaction.commit()
        
        action = self.daoFactory(classname = "DBSBufferFiles.HeritageLFNParent")
        action.execute(parentLFNs = parentLFNs, childID = self["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        return
    
    def updateLocations(self):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        if len(self["newlocations"]) == 0:
            return

        existingTransaction = self.beginTransaction()

        insertAction = self.daoFactory(classname = "DBSBufferFiles.AddLocation")
        nameMap = insertAction.execute(siteName = self["newlocations"],
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

        binds = []
        for location in self["newlocations"]:
            binds.append({"fileid": self["id"],
                          "locationid": nameMap[location]})

        addAction = self.daoFactory(classname = "DBSBufferFiles.SetLocation")
        addAction.execute(binds = binds,
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        for siteName in nameMap.keys():
            self["locations"].add(siteName)
            
        self["newlocations"].clear()
        self.commitTransaction(existingTransaction)
            
        return
        
    def setLocation(self, se, immediateSave = True):
        """
        _setLocation_
        
        Sets the location of a file. If immediateSave is True, commit change to
        the DB immediately, otherwise queue for addition when save() is called.
        """
        if isinstance(se, str):
            self["newlocations"].add(se)
            self["locations"].add(se)
        else:
            self["newlocations"].update(se)
            self["locations"].update(se)

        if immediateSave:
            self.updateLocations()

        return

    def setAlgorithm(self, appName = None, appVer = None, appFam = None,
                     psetHash = None, configContent = None):
        """
        _setAlgorithm_

        Set the DBS algorithm for this file.
        """
        self["appName"] = appName
        self["appVer"] = appVer
        self["appFam"] = appFam
        self["psetHash"] = psetHash
        self["configContent"] = configContent
        return

    def setDatasetPath(self, datasetPath):
        """
        _setDatasetPath_

        Set the dataset path for this file.
        """
        self["datasetPath"] = datasetPath
        return

    def getParentLFNs(self):
        """
        Get a flat list of parent LFNs
        """
        result = []
        parents = self['parents']
        while parents:
            result.extend(parents)
            temp = []
            for parent in parents:
                temp.extend(parent["parents"])
            parents = temp
        result.sort()   # ensure SecondaryInputFiles are in order
        return [x['lfn'] for x in result]

    def addRunSet(self, runSet):
        """
        add the set of runs.  This should be called after a file is created,
        unlike addRun which should be called before the file was created.
        runSet should be set of DataStruct.Run
        also there should be no duplicate entries in runSet.
        (May need to change in schema level not to allow duplicate record)
        """
        existingTransaction = self.beginTransaction()

        lumiAction = self.daoFactory(classname = "DBSBufferFiles.AddRunLumi")
        lumiAction.execute(file = self["lfn"], runs = runSet,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        action = self.daoFactory(classname = "DBSBufferFiles.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getDBConn(), 
                              transaction = self.existingTransaction())

        self["runs"].clear()
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        self.commitTransaction(existingTransaction)
        return

    def setBlock(self, blockName):
        """
        _setBlock_

        Associate this file with a block in DBS/PhEDEx.
        """
        existingTransaction = self.beginTransaction()
        
        blockAction = self.daoFactory(classname = "DBSBufferFiles.SetBlock")
        blockAction.execute(self["lfn"], blockName, conn = self.getDBConn(), 
                              transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
