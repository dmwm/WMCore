#!/usr/bin/env python
"""
_WorkUnit_

Represents a work unit in WMBS
"""

from __future__ import absolute_import, division, print_function

import sys
import time

from WMCore.DataStructs.WorkUnit import WorkUnit as DSWorkUnit
from WMCore.WMBS.WMBSBase import WMBSBase


class WorkUnit(WMBSBase, DSWorkUnit):
    """
    A simple object representing a file in WMBS
    """

    def __init__(self, wuid=None, taskID=None, retryCount=0, lastUnitCount=None, lastSubmitTime=int(time.time()),
                 status=0, firstEvent=1, lastEvent=sys.maxsize, fileid=None, runLumi=None):
        WMBSBase.__init__(self)
        DSWorkUnit.__init__(self, taskID=taskID, retryCount=retryCount, lastUnitCount=lastUnitCount,
                            lastSubmitTime=lastSubmitTime, status=status, firstEvent=firstEvent, lastEvent=lastEvent,
                            fileid=fileid, runLumi=runLumi)
        self.setdefault('id', wuid)

    def exists(self):
        """
        If id is given, check with id or check with taskid, fileid, run/lumi
        """

        if self['id'] > 0:
            action = self.daofactory(classname='WorkUnit.ExistsByID')
            result = action.execute(wuid=self['id'], conn=self.getDBConn(), transaction=self.existingTransaction())
        elif self['taskid'] and self['fileid'] and self['run_lumi']:
            action = self.daofactory(classname='WorkUnit.ExistsByTaskFileLumi')
            result = action.execute(taskid=self['taskid'], fileid=self['fileid'], run_lumi=self['run_lumi'],
                                    conn=self.getDBConn(), transaction=self.existingTransaction())
        else:
            raise NotImplementedError("No way to look up existence without ID or task, file, run, and lumi")

        return result

    def getInfo(self):
        """
        Return the WorkUnit attributes as a tuple based off of DataStructs
        """
        dsInfo = super(WorkUnit, self).getInfo()
        return (self['id'],) + dsInfo

    # def getLocations(self):
    #     """
    #     Get a list of locations for this file
    #     """
    #     return list(self['locations'])
    #
    # def getRuns(self):
    #     """
    #     Get a list of run lumi objects (List of set() of type
    #     WMCore.DataStructs.Run)
    #     """
    #     return list(self['runs'])
    #
    # def getParentLFNs(self):
    #     """
    #     Get a flat list of parent LFNs
    #     """
    #     result = []
    #     parents = self['parents']
    #     while parents:
    #         result.extend(parents)
    #         temp = []
    #         for parent in parents:
    #             temp.extend(parent["parents"])
    #         parents = temp
    #     result.sort()   # ensure SecondaryInputFiles are in order
    #     return [x['lfn'] for x in result]
    #
    # def getAncestors(self, level = 2, type = "id"):
    #     """
    #     Get ancestorLFNs. it will access directly DAO.
    #     level indicates the level of ancestors. default value is 2
    #     (grand parents). level should be bigger than >= 1
    #     """
    #     existingTransaction = self.beginTransaction()
    #
    #     def _getAncestorIDs(ids, level):
    #         action = self.daofactory(classname = "Files.GetParentIDsByID")
    #         parentIDs = action.execute(ids, conn = self.getDBConn(),
    #                                    transaction = self.existingTransaction())
    #
    #         parentIDs.sort()
    #         if level == 1 or len(parentIDs) == 0:
    #             return parentIDs
    #         else:
    #             return _getAncestorIDs(parentIDs, level - 1)
    #
    #     if self["id"] < 0:
    #         self.load()
    #
    #     idList = _getAncestorIDs(self["id"], level)
    #
    #     if type == "id":
    #         results = idList
    #     elif type == "lfn":
    #         results = []
    #         for fileID in idList:
    #             anceFile = File(id=fileID)
    #             anceFile.load()
    #             results.append(anceFile["lfn"])
    #     elif type == "file":
    #         results = []
    #         for fileID in idList:
    #             anceFile = File(id=fileID)
    #             anceFile.load()
    #             results.append(anceFile)
    #
    #     self.commitTransaction(existingTransaction)
    #     return results
    #
    # def getDescendants(self, level=2, type="id"):
    #     """
    #     Get descendants. it will access directly DAO.
    #     level indicates the level of ancestors. default value is 2
    #     (grand parents). level should be bigger than >= 1
    #     """
    #     existingTransaction = self.beginTransaction()
    #
    #     def _getDescendantIDs(ids, level):
    #         action = self.daofactory(classname = "Files.GetChildIDsByID")
    #         childIDs = action.execute(ids, conn = self.getDBConn(),
    #                                    transaction = self.existingTransaction())
    #
    #         childIDs.sort()
    #         if level == 1 or len(childIDs) == 0:
    #             return childIDs
    #         else:
    #             return _getDescendantIDs(childIDs, level - 1)
    #
    #     if self["id"] < 0:
    #         self.load()
    #
    #     idList = _getDescendantIDs(self["id"], level)
    #
    #     if type == "id":
    #         results = idList
    #     elif type == "lfn":
    #         results = []
    #         for fileID in idList:
    #             descFile = File(id=fileID)
    #             descFile.load()
    #             results.append(descFile['lfn'])
    #     elif type == "file":
    #         results = []
    #         for fileID in idList:
    #             descFile = File(id=fileID)
    #             descFile.load()
    #             results.append(descFile)
    #
    #     self.commitTransaction(existingTransaction)
    #     return results

    def load(self):
        """
        _load_

        Load any meta data that is associated with a WorkUnit.
        """

        if self['id'] > 0:
            action = self.daofactory(classname="WorkUnit.GetByID")
            result = action.execute(self['id'], conn=self.getDBConn(), transaction=self.existingTransaction())
        elif self['taskid'] and self['fileid'] and self['run_lumi']:
            action = self.daofactory(classname='WorkUnit.GetByTaskFileLumi')
            result = action.execute(taskid=self['taskid'], fileid=self['fileid'], run_lumi=self['run_lumi'],
                                    conn=self.getDBConn(), transaction=self.existingTransaction())
        else:
            raise NotImplementedError("Only methods to get a work unit is by ID or by taskid, fileid, run, lumi")

        self.update(result)
        return

    # def loadData(self, parentage=0):
    #     """
    #     _loadData_
    #
    #     Load all information about a file.  This currently includes meta data,
    #     the run and lumi information, all the locations that where the file
    #     is stored and any parentage information.  The parentage parameter to
    #     this method will determine how many generations to load.
    #     """
    #     existingTransaction = self.beginTransaction()
    #
    #     if self["id"] < 0 or self["lfn"] == "":
    #         self.load()
    #
    #     action = self.daofactory(classname="Files.GetRunLumiFile")
    #     runs = action.execute(self["lfn"], conn=self.getDBConn(),
    #                           transaction=self.existingTransaction())
    #
    #     [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]
    #
    #     action = self.daofactory(classname="Files.GetLocation")
    #     self["locations"] = action.execute(self["lfn"], conn=self.getDBConn(),
    #                                        transaction=self.existingTransaction())
    #     self["newlocations"].clear()
    #     self["parents"] = set()
    #
    #     if parentage > 0:
    #         action = self.daofactory(classname="Files.GetParents")
    #         parentLFNs = action.execute(self["lfn"], conn=self.getDBConn(),
    #                                     transaction=self.existingTransaction())
    #
    #         for lfn in parentLFNs:
    #             f = File(lfn=lfn)
    #             f.load()
    #             f.loadData(parentage=parentage - 1)
    #             self["parents"].add(f)
    #
    #     self.commitTransaction(existingTransaction)
    #     return

    def create(self):
        """
        _create_

        Create a work unit.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """

        # TODO: Allow creation of a work unit without passing in ID

        existingTransaction = self.beginTransaction()

        if self.exists() is not False:
            self.commitTransaction(existingTransaction)
            self.load()
            # assume if the file already exist, parentage is already set.
            # or not exist yet
            return

        addAction = self.daofactory(classname="WorkUnit.Add")

        addAction.execute(taskid=self['taskid'], retry_count=self['retry_count'],
                          last_unit_count=self['last_unit_count'], last_submit_time=self['last_submit_time'],
                          status=self['status'],
                          first_event=self['firstevent'], last_event=self['lastevent'],
                          fileid=self['fileid'], run=self['run_lumi'].run, lumi=self['run_lumi'].lumis[0],
                          conn=self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return

    def delete(self):
        """
        Remove a WorkUnit from WMBS
        """
        action = self.daofactory(classname="WorkUnit.Delete")
        action.execute(wuid=self["id"], conn=self.getDBConn(), transaction=self.existingTransaction())
        return

        # def addChild(self, lfn):
        #     """
        #     set an existing file (lfn) as a child of this file
        #     """
        #     existingTransaction = self.beginTransaction()
        #
        #     child = File(lfn=lfn)
        #     child.load()
        #
        #     if not self['id'] > 0:
        #         raise Exception("Parent file doesn't have an id %s" % self['lfn'])
        #     if not child['id'] > 0:
        #         raise Exception("Child file doesn't have an id %s" % child['lfn'])
        #
        #     heritageAction = self.daofactory(classname="Files.Heritage")
        #     heritageAction.execute(child=child["id"], parent=self["id"],
        #                            conn=self.getDBConn(),
        #                            transaction=self.existingTransaction())
        #
        #     self.commitTransaction(existingTransaction)
        #     return
        #
        # def addParent(self, lfn):
        #     """
        #     set an existing file (lfn) as a parent of this file
        #     """
        #     existingTransaction = self.beginTransaction()
        #
        #     parent = File(lfn=lfn)
        #     parent.load()
        #     self["parents"].add(parent)
        #
        #     if not self["id"] > 0:
        #         raise Exception("Child file doesn't have an id %s" % self["lfn"])
        #     if not parent["id"] > 0:
        #         raise Exception("Parent file doesn't have an id %s" % \
        #                         parent["lfn"])
        #
        #     action = self.daofactory(classname="Files.Heritage")
        #     action.execute(child=self["id"], parent=parent["id"],
        #                    conn=self.getDBConn(),
        #                    transaction=self.existingTransaction())
        #
        #     self.commitTransaction(existingTransaction)
        #     return
        #
        # def addRunSet(self, runSet):
        #     """
        #     add the set of runs.  This should be called after a file is created,
        #     unlike addRun which should be called before the file was created.
        #     runset should be set of DataStruct.Run
        #     also there should be no duplicate entries in runset.
        #     (May need to change in schema level not to allow duplicate record)
        #     """
        #     existingTransaction = self.beginTransaction()
        #
        #     lumiAction = self.daofactory(classname="Files.AddRunLumi")
        #     lumiAction.execute(file=self["lfn"], runs=runSet,
        #                        conn=self.getDBConn(),
        #                        transaction=self.existingTransaction())
        #
        #     action = self.daofactory(classname="Files.GetRunLumiFile")
        #     runs = action.execute(self["lfn"], conn=self.getDBConn(),
        #                           transaction=self.existingTransaction())
        #
        #     self["runs"].clear()
        #     [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]
        #
        #     self.commitTransaction(existingTransaction)
        #     return
        #
        # def updateLocations(self):
        #     """
        #     _updateLocations_
        #
        #     Write any new locations to the database.  After any new locations are
        #     written to the database all locations will be reloaded from the
        #     database.
        #     """
        #     if not self.exists():
        #         return
        #
        #     existingTransaction = self.beginTransaction()
        #
        #     # Add new locations if required
        #     if len(self["newlocations"]) > 0:
        #         addAction = self.daofactory(classname="Files.SetLocation")
        #         addAction.execute(file=self["id"], location=self["newlocations"],
        #                           conn=self.getDBConn(),
        #                           transaction=self.existingTransaction())
        #
        #     # Update locations from the DB
        #     getAction = self.daofactory(classname="Files.GetLocation")
        #     self["locations"] = getAction.execute(self["lfn"], conn=self.getDBConn(),
        #                                           transaction=self.existingTransaction())
        #     self["newlocations"].clear()
        #
        #     self.commitTransaction(existingTransaction)
        #     return
        #
        # def setLocation(self, pnn, immediateSave=True):
        #     """
        #     Sets the location of a file. If immediateSave is True, commit change to
        #     the DB immediately, otherwise queue for addition when save() is called.
        #     Also removes previous error where a file would have to be saved before
        #     locations could be added - confusing when file requires locations on its
        #     first creation (breaks transaction model in Fileset commits etc)
        #     """
        #     if isinstance(pnn, str):
        #         self['newlocations'].add(pnn)
        #         self['locations'].add(pnn)
        #     else:
        #         self['newlocations'].update(pnn)
        #         self['locations'].update(pnn)
        #
        #     if immediateSave:
        #         self.updateLocations()
        #
        #     return
        #
        # def setCksum(self, cksum, cktype):
        #     """
        #     _setCKType_
        #
        #     Set the Checksum Type
        #     """
        #
        #     myThread = threading.currentThread()
        #
        #     if self['id'] < 0:
        #         # You haven't bothered to create the file!
        #         return
        #
        #     existingTransaction = self.beginTransaction()
        #
        #     action = self.daofactory(classname="Files.AddChecksum")
        #     action.execute(fileid=self['id'], cktype=cktype, cksum=cksum, \
        #                    conn=self.getDBConn(), transaction=existingTransaction)
        #
        #     self.commitTransaction(existingTransaction)
        #
        #     return
        #
        # def loadChecksum(self):
        #     """
        #     _loadChecksum_
        #
        #     Grab checksums.  If plural, put a list of dictionaries of type
        #     {'cktype', 'cksum'} in both self['cktype'] and self['cksum']
        #     """
        #     # Now get the checksum
        #     action = self.daofactory(classname='Files.GetChecksum')
        #     result = action.execute(fileid=self['id'], conn=self.getDBConn(),
        #                             transaction=self.existingTransaction())
        #     if result:
        #         self.update(result)
        #
        #     return
        #
        # def loadFromDataStructsFile(self, file):
        #     """
        #     _loadFromDataStructsFile_
        #
        #     This function will create a WMBS File given a DataStructs file
        #     """
        #     self.update(file)
        #     # Clear the parents since update
        #     # will update the parents with set of lfns,
        #     # parents should be set of wmbs files in WMBS File class
        #     self["parents"] = set()
        #
        #     if type(file["locations"]) == set:
        #         s = file["locations"].copy()
        #         pnn = s.pop()
        #     elif type(file["locations"]) == list:
        #         pnn = file["locations"][0]
        #     else:
        #         pnn = file["locations"]
        #
        #     self.setLocation(pnn=pnn, immediateSave=False)
        #
        #     self.create()
        #
        #     for parent in file['parents']:
        #         self.addParent(parent)
        #
        #     return
        #
        # def returnDataStructsFile(self):
        #     """
        #     _returnDataStructsFile_
        #
        #     Creates a dataStruct file out of this file
        #     """
        #     parents = set()
        #     for parent in self["parents"]:
        #         parents.add(WMFile(lfn=parent['lfn'], size=parent['size'],
        #                            events=parent['events'], checksums=parent['checksums'],
        #                            parents=parent['parents'], merged=parent['merged']))
        #
        #     file = WMFile(lfn=self['lfn'], size=self['size'],
        #                   events=self['events'], checksums=self['checksums'],
        #                   parents=parents, merged=self['merged'])
        #
        #     for run in self['runs']:
        #         file.addRun(run)
        #
        #     for location in self['locations']:
        #         file.setLocation(pnn=location)
        #
        #     return file
