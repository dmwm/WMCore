#!/usr/bin/env python
"""
Fileset.py

Created by Dave Evans on 2010-03-18.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from WMCore.DataStructs.WMObject import WMObject


class Fileset(dict, WMObject):
    """
    _Fileset_

    """

    def __init__(self, **options):
        dict.__init__(self)
        WMObject.__init__(self)
        self.setdefault("name", None)
        self.setdefault("files", {})
        self.update(options)
        self.collection = None

    def setCollection(self, collection):
        """
        _setCollection_

        Associate this fileset with a collection.
        """
        self.collectionName = collection["name"]
        self.collectionType = collection["type"]
        return

    def create(self):
        """
        _create_

        create a new fileset within a collection
        """
        pass

    def populate(self):
        """
        _populate_

        populate information about this fileset
        """
        pass

    def drop(self):
        """
        _drop_

        Remove the fileset from its collection
        """
        pass

    def add(self, files, mask):
        """
        _add_

        Add files to this fileset
        files should be a list of WMCore.DataStruct.File objects

        """
        pass

    def listFiles(self):
        """
        _listFiles_

        Iterate/yield the files in this fileset
        """
        pass

    def fileCount(self):
        """
        _fileCount_

        Total number of files in this fileset
        """
        pass

    def fileset(self):
        """
        _fileset_

        Create and return an instance of a WMCore.DataStructs.Fileset
        containing the files in this (ACDC) fileset

        """
        pass
