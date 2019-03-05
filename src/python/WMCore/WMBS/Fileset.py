#!/usr/bin/env python
# Turn off to many arguments
# pylint: disable=R0913
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription
"""

import logging

from WMCore.DataStructs.Fileset import Fileset as WMFileset
from WMCore.WMBS.File import File, addFilesToWMBSInBulk
from WMCore.WMBS.WMBSBase import WMBSBase


class Fileset(WMBSBase, WMFileset):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a
    complete block, a block in transfer, some user defined dataset, a
    many file lumi-section etc.

    workflow + fileset = subscription
    """

    def __init__(self, name=None, id=-1, is_open=True, files=None,
                 parents=None, parents_open=True, source=None, sourceUrl=None):
        WMBSBase.__init__(self)
        WMFileset.__init__(self, name=name, files=files)

        if parents is None:
            parents = set()

        # Create a new fileset
        self.id = id
        self.open = is_open
        self.parents = parents
        self.setParentage(parents, parents_open)
        self.source = source
        self.sourceUrl = sourceUrl
        self.lastUpdate = 0

    def setLastUpdate(self, timeUpdate):
        """
        _setLastUpdate_

        Change the last update time of this fileset.  The lastUpdate parameter is a int
        representing the last time where the fileset was modifed.
        """
        closeAction = self.daofactory(classname="Fileset.SetLastUpdate")
        closeAction.execute(fileset=self.name, timeUpdate=timeUpdate, conn=self.getDBConn(),
                            transaction=self.existingTransaction())

        self.lastUpdate = timeUpdate
        return

    def addFile(self, file):
        """
        Add the file object to the set, but don't commit to the database
        Call commit() to do that - enables bulk operations
        """
        WMFileset.addFile(self, file)

    def setParentage(self, parents, parents_open):
        """
        Set parentage for this fileset - set parents to closed
        """
        if parents:
            for parent in parents:
                if isinstance(parent, Fileset):
                    self.parents.add(parent)
                else:
                    self.parents.add(Fileset(name=parent,
                                             is_open=parents_open,
                                             parents_open=False))

    def exists(self):
        """
        Does a fileset exist with this name in the database
        """
        if self.id != -1:
            action = self.daofactory(classname="Fileset.ExistsByID")
            result = action.execute(id=self.id, conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Fileset.Exists")
            result = action.execute(self.name, conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
            if result is not False:
                self.id = result

        return result

    def create(self):
        """
        Add the new fileset to WMBS, and commit the files
        """
        if self.exists() is not False:
            self.load()
            return

        existingTransaction = self.beginTransaction()
        createAction = self.daofactory(classname="Fileset.New")
        createAction.execute(self.name, self.open, conn=self.getDBConn(),
                             transaction=self.existingTransaction())
        self.commit()
        self.loadData()
        self.commitTransaction(existingTransaction)
        logging.info("Fileset created: %s", self.name)
        return

    def delete(self):
        """
        Remove this fileset from WMBS
        """
        action = self.daofactory(classname="Fileset.Delete")
        result = action.execute(name=self.name, conn=self.getDBConn(),
                                transaction=self.existingTransaction())

        return result

    def load(self):
        """
        _load_

        Load the name, id and time that fileset was last updated in the
        database.
        """
        if self.id > 0:
            action = self.daofactory(classname="Fileset.LoadFromID")
            result = action.execute(fileset=self.id,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Fileset.LoadFromName")
            result = action.execute(fileset=self.name,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.id = result["id"]
        self.name = result["name"]
        self.open = result["open"]
        self.lastUpdate = result["last_update"]

        return self

    def loadData(self, parentage=1):
        """
        _loadData_

        Load all the files that belong to this fileset.
        """
        existingTransaction = self.beginTransaction()

        if self.name is None or self.id < 0:
            self.load()

        action = self.daofactory(classname="Files.InFileset")
        results = action.execute(fileset=self.id,
                                 conn=self.getDBConn(),
                                 transaction=self.existingTransaction())

        self.files = set()
        self.newfiles = set()

        for result in results:
            thisFile = File(id=result["fileid"])
            thisFile.loadData(parentage=parentage)
            self.files.add(thisFile)

        self.commitTransaction(existingTransaction)
        return

    def commit(self):
        """
        Add contents of self.newfiles to the database,
        empty self.newfiles, reload self
        """
        existingTransaction = self.beginTransaction()

        if not self.exists():
            self.create()

        ids = []
        while len(self.newfiles) > 0:
            # Check file objects exist in the database, save those that don't
            f = self.newfiles.pop()
            if not f.exists():
                f.create()
            ids.append(f["id"])
            self.files.add(f)

        # Add Files to DB only if there are any files on newfiles
        if len(ids) > 0:
            addAction = self.daofactory(classname="Files.AddToFilesetByIDs")
            addAction.execute(file=ids, fileset=self.id,
                              conn=self.getDBConn(),
                              transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def markOpen(self, isOpen):
        """
        _markOpen_

        Change the open status of this fileset.  The isOpen parameter is a bool
        representing whether or not the fileset is open.
        """
        closeAction = self.daofactory(classname="Fileset.MarkOpen")
        closeAction.execute(fileset=self.name, isOpen=isOpen,
                            conn=self.getDBConn(),
                            transaction=self.existingTransaction())
        self.open = isOpen

        return

    def __str__(self):
        """
        __str__

        Write out something useful because Fileset doesn't
        inherit from dict
        """

        st = {'name': self.name, 'files': self.files, 'id': self.id,
              'open': self.open, 'parents': self.parents,
              'lastUpdate': self.lastUpdate}

        return str(st)

    def addFilesToWMBSInBulk(self, files, workflowName, isDBS=True):
        """
        _addFilesToWMBSInBulk

        Do a bulk addition of files into WMBS. This is a speedup.
        """
        # Can / should we move this to commit???
        files = addFilesToWMBSInBulk(self.id, workflowName, files,
                                     isDBS=isDBS,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())
        return files
