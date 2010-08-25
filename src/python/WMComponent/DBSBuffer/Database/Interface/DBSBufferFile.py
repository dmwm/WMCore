#!/usr/bin/env python
"""
_DBSBufferFile_

A simple object representing a file in WMBS
"""

__revision__ = "$Id: DBSBufferFile.py,v 1.4 2009/09/22 19:49:22 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from sets import Set

from WMCore.DataStructs.File import File as WMFile
from WMCore.DAOFactory import DAOFactory

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
        self.daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        return

    def exists(self):
        """
        _exists_

        Determine whether or not a file with this LFN exists inside the
        database.  Return the file's ID if it exists, False otherwise.
        """
        action = self.daofactory(classname = "DBSBufferFiles.Exists")
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
            action = self.daofactory(classname = "DBSBufferFiles.GetByID")
            result = action.execute(self["id"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "DBSBufferFiles.GetByLFN")
            result = action.execute(self["lfn"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        self.update(result)

        action = self.daofactory(classname = "DBSBufferFiles.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname = "DBSBufferFiles.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn = self.getDBConn(),
                                           transaction = self.existingTransaction()) 

        self["newlocations"].clear()
        self["parents"].clear()
        
        if parentage > 0:
            action = self.daofactory(classname = "DBSBufferFiles.GetParents")
            lfns = action.execute(self["lfn"], conn = self.getDBConn(),
                                  transaction = self.existingTransaction())
            for lfn in lfns:
                parentFile = DBSBufferFile(lfn = lfn)
                parentFile.load(parentage = parentage - 1)
                self["parents"].add(parentFile)

        self.commitTransaction(existingTransaction)
        return
    
    def create(self):
        """
        _create_

        """
        existingTransaction = self.beginTransaction()

        if self.exists() != False:
            self.load()
            return

        algoAction = self.daofactory(classname = "NewAlgo")
        algoAction.execute(appName = self["appName"], appVer = self["appVer"],
                           appFam = self["appFam"], psetHash = self["psetHash"],
                           configContent = self["configContent"])

        datasetAction = self.daofactory(classname = "NewDataset")
        datasetAction.execute(datasetPath = self["datasetPath"])

        assocAction = self.daofactory(classname = "AlgoDatasetAssoc")
        assocID = assocAction.execute(appName = self["appName"], appVer = self["appVer"],
                                      appFam = self["appFam"], psetHash = self["psetHash"],
                                      datasetPath = self["datasetPath"])

        addAction = self.daofactory(classname = "DBSBufferFiles.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum= self["cksum"],
                          datasetAlgo = assocID, status = self["status"],
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        if len(self["runs"]) > 0:        
            lumiAction = self.daofactory(classname="DBSBufferFiles.AddRunLumi")
            lumiAction.execute(file = self["lfn"], runs = self["runs"],
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())
        
        self.updateLocations()
        self["id"] = self.exists()
        self.commitTransaction(existingTransaction)
        return
    
    def delete(self):
        """
        _delete_
        
        Remove a file from the DSBuffer database.
        """
        action = self.daofactory(classname = "DBSBufferFiles.Delete")
        action.execute(file = self["lfn"], conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        return
        
    def addChild(self, lfn):
        """
        _addChild_
        
        Set an existing file (lfn) as a child of this file.
        """
        existingTransaction = self.beginTransaction()

        child = DBSBufferFile(lfn = lfn)
        child.load()
        
        if not self["id"] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self["lfn"]
        if not child["id"] > 0:
            raise Exception, "Child file doesn't have an id %s" % child["lfn"]

        action = self.daofactory(classname = "DBSBufferFiles.Heritage")
        action.execute(child = child["id"], parent = self["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
        
    def addParent(self, lfn):
        """
        _addParent_
        
        Set an existing file (lfn) as a parent of this file.
        """
        existingTransaction = self.beginTransaction()

        parent = DBSBufferFile(lfn = lfn)
        parent.load()
        self["parents"].add(parent)

        if not self["id"] > 0:
            raise Exception, "Child file doesn't have an id %s" % self["lfn"]
        if not parent["id"] > 0:
            raise Exception, "Parent file doesn't have an id %s" % \
                  parent["lfn"]
        
        action = self.daofactory(classname = "DBSBufferFiles.Heritage")
        action.execute(child = self["id"], parent = parent["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
    
    def updateLocations(self):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        existingTransaction = self.beginTransaction()

        if len(self["newlocations"]) > 0:
            insertAction = self.daofactory(classname = "DBSBufferFiles.AddLocation")
            insertAction.execute(siteName = self["newlocations"],
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction())

            addAction = self.daofactory(classname = "DBSBufferFiles.SetLocation")
            addAction.execute(file = self["lfn"], location = self["newlocations"],
                              conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        getAction = self.daofactory(classname = "DBSBufferFiles.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], conn = self.getDBConn(),
                                              transaction = self.existingTransaction())

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

        lumiAction = self.daofactory(classname = "DBSBufferFiles.AddRunLumi")
        lumiAction.execute(file = self["lfn"], runs = runSet,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        action = self.daofactory(classname = "DBSBufferFiles.GetRunLumiFile")
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
        
        blockAction = self.daofactory(classname = "DBSBufferFiles.SetBlock")
        blockAction.execute(self["lfn"], blockName, conn = self.getDBConn(), 
                              transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
