#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS.
"""

from builtins import next, str, bytes

import logging
import threading

from WMCore.DataStructs.File import File as WMFile
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.WMBSBase import WMBSBase


class File(WMBSBase, WMFile):
    """
    A simple object representing a file in WMBS
    """

    def __init__(self, lfn="", id=-1, size=0, events=0, checksums={},
                 parents=None, locations=None, first_event=0,
                 last_event=0, merged=True):
        WMBSBase.__init__(self)
        WMFile.__init__(self, lfn=lfn, size=size, events=events,
                        checksums=checksums, parents=parents, merged=merged)

        if locations is None:
            self.setdefault("newlocations", set())
        else:
            if isinstance(locations, (str, bytes)):
                self.setdefault("newlocations", set())
                self['newlocations'].add(locations)
            else:
                self.setdefault("newlocations", locations)

        # overwrite the default value set from the WMFile
        self["first_event"] = first_event
        self["last_event"] = last_event
        self.setdefault("id", id)
        self['locations'] = set()

    def exists(self):
        """
        if id is exist (not -1) check with id first
        Does a file exist with this lfn, return the id
        """
        if self["id"] != -1:
            action = self.daofactory(classname="Files.ExistsByID")
            result = action.execute(id=self["id"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Files.Exists")
            result = action.execute(lfn=self["lfn"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
            if result:
                self["id"] = result

        return result

    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return (self['lfn'], self['id'], self['size'], self['events'],
                self['checksums'], list(self['runs']), list(self['locations']),
                list(self['parents']))

    def getLocations(self):
        """
        Get a list of locations for this file
        """
        return list(self['locations'])

    def getRuns(self):
        """
        Get a list of run lumi objects (List of set() of type
        WMCore.DataStructs.Run)
        """
        return list(self['runs'])

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
        result.sort()  # ensure SecondaryInputFiles are in order
        return [x['lfn'] for x in result]

    def getAncestors(self, level=2, type="id"):
        """
        Get ancestorLFNs. it will access directly DAO.
        level indicates the level of ancestors. default value is 2
        (grand parents). level should be bigger than >= 1
        """
        existingTransaction = self.beginTransaction()

        def _getAncestorIDs(ids, level):
            action = self.daofactory(classname="Files.GetParentIDsByID")
            parentIDs = sorted(action.execute(ids, conn=self.getDBConn(), transaction=self.existingTransaction()))

            if level == 1 or len(parentIDs) == 0:
                return parentIDs
            else:
                return _getAncestorIDs(parentIDs, level - 1)

        if self["id"] < 0:
            self.load()

        idList = _getAncestorIDs(self["id"], level)

        if type == "id":
            results = idList
        elif type == "lfn":
            results = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                results.append(anceFile["lfn"])
        elif type == "file":
            results = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                results.append(anceFile)

        self.commitTransaction(existingTransaction)
        return results

    def getDescendants(self, level=2, type="id"):
        """
        Get descendants. it will access directly DAO.
        level indicates the level of ancestors. default value is 2
        (grand parents). level should be bigger than >= 1
        """
        existingTransaction = self.beginTransaction()

        def _getDescendantIDs(ids, level):
            action = self.daofactory(classname="Files.GetChildIDsByID")
            childIDs = sorted(action.execute(ids, conn=self.getDBConn(), transaction=self.existingTransaction()))

            if level == 1 or len(childIDs) == 0:
                return childIDs
            else:
                return _getDescendantIDs(childIDs, level - 1)

        if self["id"] < 0:
            self.load()

        idList = _getDescendantIDs(self["id"], level)

        if type == "id":
            results = idList
        elif type == "lfn":
            results = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                results.append(descFile['lfn'])
        elif type == "file":
            results = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                results.append(descFile)

        self.commitTransaction(existingTransaction)
        return results

    def load(self):
        """
        _load_

        Load any meta data that is associated with a file.  This currently
        includes id, lfn, size, events and cksum.
        """
        if self["id"] > 0:
            action = self.daofactory(classname="Files.GetByID")
            result = action.execute(self["id"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Files.GetByLFN")
            result = action.execute(self["lfn"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.update(result)
        self.loadChecksum()
        return

    def loadData(self, parentage=0):
        """
        _loadData_

        Load all information about a file.  This currently includes meta data,
        the run and lumi information, all the locations where the file
        is stored and any parentage information.  The parentage parameter to
        this method will determine how many generations to load.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] < 0 or self["lfn"] == "":
            self.load()

        action = self.daofactory(classname="Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

        [self.addRun(run=Run(r, *runs[r])) for r in runs]

        action = self.daofactory(classname="Files.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
        self["newlocations"].clear()
        self["parents"] = set()

        if parentage > 0:
            action = self.daofactory(classname="Files.GetParents")
            parentLFNs = action.execute(self["lfn"], conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

            for lfn in parentLFNs:
                f = File(lfn=lfn)
                f.load()
                f.loadData(parentage=parentage - 1)
                self["parents"].add(f)

        self.commitTransaction(existingTransaction)
        return

    def create(self):
        """
        _create_

        Create a file.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """
        existingTransaction = self.beginTransaction()

        if self.exists():
            self.commitTransaction(existingTransaction)
            self.load()
            # assume if the file already exist, parentage is already set.
            # or not exist yet
            return

        addAction = self.daofactory(classname="Files.Add")
        addAction.execute(files=self["lfn"], size=self["size"],
                          events=self["events"],
                          first_event=self["first_event"],
                          merged=self["merged"],
                          conn=self.getDBConn(),
                          transaction=self.existingTransaction())

        if len(self["runs"]) > 0:
            lumiAction = self.daofactory(classname="Files.AddRunLumi")
            lumiAction.execute(file=self["lfn"], runs=self["runs"],
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())
        self.load()
        self.updateLocations()
        # call it here to make sure self["id"] exist
        if self["parents"]:
            for parent in self['parents']:
                parent.create()
                action = self.daofactory(classname="Files.Heritage")
                action.execute(child=self["id"], parent=parent["id"],
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        if self['checksums']:
            # Add a checksum
            for entry in self['checksums']:
                self.setCksum(cksum=self['checksums'][entry], cktype=entry)
        return

    def delete(self):
        """
        Remove a file from WMBS
        """
        action = self.daofactory(classname="Files.Delete")
        action.execute(file=self["lfn"], conn=self.getDBConn(),
                       transaction=self.existingTransaction())
        return

    def addChild(self, lfn):
        """
        set an existing file (lfn) as a child of this file
        """
        existingTransaction = self.beginTransaction()

        child = File(lfn=lfn)
        child.load()

        if not self['id'] > 0:
            raise Exception("Parent file doesn't have an id %s" % self['lfn'])
        if not child['id'] > 0:
            raise Exception("Child file doesn't have an id %s" % child['lfn'])

        heritageAction = self.daofactory(classname="Files.Heritage")
        heritageAction.execute(child=child["id"], parent=self["id"],
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def addParent(self, lfn):
        """
        set an existing file (lfn) as a parent of this file
        """
        existingTransaction = self.beginTransaction()

        parent = File(lfn=lfn)
        parent.load()
        self["parents"].add(parent)

        if not self["id"] > 0:
            raise Exception("Child file doesn't have an id %s" % self["lfn"])
        if not parent["id"] > 0:
            raise Exception("Parent file doesn't have an id %s" % parent["lfn"])

        action = self.daofactory(classname="Files.Heritage")
        action.execute(child=self["id"], parent=parent["id"],
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def addRunSet(self, runSet):
        """
        add the set of runs.  This should be called after a file is created,
        unlike addRun which should be called before the file was created.
        runset should be set of DataStruct.Run
        also there should be no duplicate entries in runset.
        (May need to change in schema level not to allow duplicate record)
        """
        existingTransaction = self.beginTransaction()

        lumiAction = self.daofactory(classname="Files.AddRunLumi")
        lumiAction.execute(file=self["lfn"], runs=runSet,
                           conn=self.getDBConn(),
                           transaction=self.existingTransaction())

        action = self.daofactory(classname="Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

        self["runs"].clear()
        [self.addRun(run=Run(r, *runs[r])) for r in runs]

        self.commitTransaction(existingTransaction)
        return

    def updateLocations(self):
        """
        _updateLocations_

        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        if not self.exists():
            return

        existingTransaction = self.beginTransaction()

        # Add new locations if required
        if len(self["newlocations"]) > 0:
            addAction = self.daofactory(classname="Files.SetLocation")
            addAction.execute(file=self["id"], pnn=self["newlocations"],
                              conn=self.getDBConn(),
                              transaction=self.existingTransaction())

        # Update locations from the DB
        getAction = self.daofactory(classname="Files.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], conn=self.getDBConn(),
                                              transaction=self.existingTransaction())
        self["newlocations"].clear()

        self.commitTransaction(existingTransaction)
        return

    def setLocation(self, pnn, immediateSave=True):
        """
        Sets the location of a file. If immediateSave is True, commit change to
        the DB immediately, otherwise queue for addition when save() is called.
        Also removes previous error where a file would have to be saved before
        locations could be added - confusing when file requires locations on its
        first creation (breaks transaction model in Fileset commits etc)
        """
        if isinstance(pnn, (str, bytes)):
            self['newlocations'].add(pnn)
            self['locations'].add(pnn)
        else:
            self['newlocations'].update(pnn)
            self['locations'].update(pnn)

        if immediateSave:
            self.updateLocations()

        return

    def setCksum(self, cksum, cktype):
        """
        _setCKType_

        Set the Checksum Type
        """

        myThread = threading.currentThread()

        if self['id'] < 0:
            # You haven't bothered to create the file!
            return

        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname="Files.AddChecksum")
        action.execute(fileid=self['id'], cktype=cktype, cksum=cksum,
                       conn=self.getDBConn(), transaction=existingTransaction)

        self.commitTransaction(existingTransaction)

        return

    def loadChecksum(self):
        """
        _loadChecksum_

        Grab checksums.  If plural, put a list of dictionaries of type
        {'cktype', 'cksum'} in both self['cktype'] and self['cksum']
        """
        # Now get the checksum
        action = self.daofactory(classname='Files.GetChecksum')
        result = action.execute(fileid=self['id'], conn=self.getDBConn(),
                                transaction=self.existingTransaction())
        if result:
            self.update(result)

        return

    def loadFromDataStructsFile(self, file):
        """
        _loadFromDataStructsFile_

        This function will create a WMBS File given a DataStructs file
        """
        self.update(file)
        # Clear the parents since update
        # will update the parents with set of lfns,
        # parents should be set of wmbs files in WMBS File class
        self["parents"] = set()

        if isinstance(file["locations"], set):
            s = file["locations"].copy()
            pnn = s.pop()
        elif isinstance(file["locations"], list):
            pnn = file["locations"][0]
        else:
            pnn = file["locations"]

        self.setLocation(pnn=pnn, immediateSave=False)

        self.create()

        for parent in file['parents']:
            self.addParent(parent)

        return

    def returnDataStructsFile(self):
        """
        _returnDataStructsFile_

        Creates a dataStruct file out of this file
        """
        parents = set()
        for parent in self["parents"]:
            parents.add(WMFile(lfn=parent['lfn'], size=parent['size'],
                               events=parent['events'], checksums=parent['checksums'],
                               parents=parent['parents'], merged=parent['merged']))

        file = WMFile(lfn=self['lfn'], size=self['size'],
                      events=self['events'], checksums=self['checksums'],
                      parents=parents, merged=self['merged'])

        for run in self['runs']:
            file.addRun(run)

        for location in self['locations']:
            file.setLocation(pnn=location)

        return file


def addFilesToWMBSInBulk(filesetId, workflowName, files, isDBS=True,
                         conn=None, transaction=None):
    """
    _addFilesToWMBSInBulk

    Do a bulk addition of files into WMBS. This is a speedup.

    Assumes files are full dao objects
    """
    if not files:
        # Nothing to do
        return 0

    daofactory = next(iter(files)).daofactory
    setParentage = daofactory(classname="Files.SetParentage")
    setFileRunLumi = daofactory(classname="Files.AddRunLumi")
    setFileLocation = daofactory(classname="Files.SetLocationForWorkQueue")
    setFileAddChecksum = daofactory(classname="Files.AddChecksumByLFN")
    addFileAction = daofactory(classname="Files.Add")
    addToFileset = daofactory(classname="Files.AddDupsToFileset")
    updateFileAction = daofactory(classname="Files.Update")

    # build up list of binds for all files then run in single transaction
    parentageBinds = []
    runLumiBinds = []
    fileCksumBinds = []
    fileLocations = []
    fileCreate = []
    fileLFNs = set()
    lfnsToCreate = set()
    lfnList = set()
    fileUpdate = []

    for wmbsFile in files:
        lfn = wmbsFile['lfn']
        lfnList.add(lfn)

        if wmbsFile.get('inFileset', True):
            fileLFNs.add(lfn)
        for parent in wmbsFile['parents']:
            parentageBinds.append({'child': lfn, 'parent': parent["lfn"]})

        selfChecksums = wmbsFile['checksums']
        if len(wmbsFile['runs']) > 0:
            runLumiBinds.append({'lfn': lfn, 'runs': wmbsFile['runs']})

        if len(wmbsFile['newlocations']) < 1:
            # Then we're in trouble
            msg = "File created in WMBS without locations!\n"
            msg += "File lfn: %s\n" % (lfn)
            logging.error(msg)
            raise RuntimeError(msg)

        for loc in wmbsFile['newlocations']:
            fileLocations.append({'lfn': lfn, 'location': loc})

        if wmbsFile.exists():
            # update events, size, first_event, merged
            fileUpdate.append([lfn,
                               wmbsFile['size'],
                               wmbsFile['events'],
                               None,
                               wmbsFile["first_event"],
                               wmbsFile['merged']])
            continue

        lfnsToCreate.add(lfn)

        if selfChecksums:
            # If we have checksums we have to create a bind
            # For each different checksum
            for entry in selfChecksums:
                fileCksumBinds.append({'lfn': lfn, 'cksum': selfChecksums[entry],
                                       'cktype': entry})

        fileCreate.append([lfn,
                           wmbsFile['size'],
                           wmbsFile['events'],
                           None,
                           wmbsFile["first_event"],
                           wmbsFile['merged']])

    if len(fileCreate) > 0:
        addFileAction.execute(files=fileCreate,
                              conn=conn,
                              transaction=transaction)
        setFileAddChecksum.execute(bulkList=fileCksumBinds,
                                   conn=conn,
                                   transaction=transaction)

    if len(fileUpdate) > 0:
        updateFileAction.execute(files=fileUpdate,
                                 conn=conn,
                                 transaction=transaction)

    if len(fileLocations) > 0:
        setFileLocation.execute(lfns=lfnList, locations=fileLocations,
                                isDBS=isDBS,
                                conn=conn,
                                transaction=transaction)
    if len(runLumiBinds) > 0:
        setFileRunLumi.execute(file=runLumiBinds,
                               conn=conn,
                               transaction=transaction)

    if len(fileLFNs) > 0:
        addToFileset.execute(file=fileLFNs,
                             fileset=filesetId,
                             workflow=workflowName,
                             conn=conn,
                             transaction=transaction)

    if len(parentageBinds) > 0:
        setParentage.execute(binds=parentageBinds,
                             conn=conn,
                             transaction=transaction)

    return len(lfnsToCreate)
