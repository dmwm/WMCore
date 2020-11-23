#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""

from __future__ import print_function
from builtins import str, map

from operator import itemgetter
from WMCore.DataStructs.WMObject import WMObject
__all__ = []


class Fileset(WMObject):
    """
    _Fileset_
    Data object that contains a set of files
    """
    def __init__(self, name=None, files=None):
        """
        Assume input files are new
        """
        self.name = name
        self.files = set()

        if files is None:
            self.newfiles = set()
        else:
            self.newfiles = files

        # assume that the fileset is open at first
        self.open = True

        # assume that the lastUpdate of fileset is 0 at first
        self.lastUpdate = 0

    def setLastUpdate(self, timeUpdate):
        """
        _setLastUpdate_

        Change the last update time of this fileset.  The lastUpdate parameter is a int
        representing the last time where the fileset was modifed.
        """
        self.lastUpdate = timeUpdate

    def addFile(self, file):
        """
        Add a (set of) file(s) to the fileset
        If the file is already in self.files update that entry
            e.g. to handle updated location
        If the file is already in self.newfiles update that entry
            e.g. to handle updated location
        Else add the file to self.newfiles
        """
        file = self.makeset(file)
        new = file - self.getFiles(type='set')
        self.newfiles = self.makeset(self.newfiles) | new

        updated = self.makeset(file) & self.getFiles(type='set')
        "updated contains the original location information for updated files"

        self.files = self.files.union(updated)

    def getFiles(self, type='list'):
        if type == 'list':
            """
            List all files in the fileset - returns a set of file objects
            sorted by lfn.
            """
            files = list(self.getFiles(type='set'))

            try:
                files.sort(key=itemgetter('lfn'))
            except Exception as e:
                print('Problem with listFiles for fileset:', self.name)
                print(files.pop())
                raise e
            return files
        elif type == 'set':
            return self.makeset(self.files) | self.makeset(self.newfiles)
        elif type == 'lfn':
            """
            All the lfn's for files in the filesets
            """
            def getLFN(file):
                return file["lfn"]
            files = list(map(getLFN, self.getFiles(type='list')))
            return files
        elif type == 'id':
            """
            All the id's for files in the filesets
            """
            def getID(file):
                return file["id"]

            files = list(map(getID, self.getFiles(type='list')))
            return files

    def listNewFiles(self):
        """
        List all files in the fileset that are new - e.g. not in the DB - returns a set
        """
        return self.newfiles

    def commit(self):
        """
        Add contents of self.newfiles to self, empty self.newfiles
        """
        self.files = self.makeset(self.files) | self.makeset(self.newfiles)
        self.newfiles = set()

    def __len__(self):
        return len(self.getFiles(type='set'))

    def __iter__(self):
        for file in self.getFiles():
            yield file

    def markOpen(self, isOpen):
        """
        _markOpen_

        Change the open status of this fileset.  The isOpen parameter is a bool
        representing whether or not the fileset is open.
        """
        self.open = isOpen

    def __str__(self):
        """
        __str__

        Write out something useful
        """

        st = {'name': self.name, 'files': self.files}

        return str(st)
