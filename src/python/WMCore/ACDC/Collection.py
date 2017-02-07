#!/usr/bin/env python
# encoding: utf-8
"""
Collection.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from WMCore.DataStructs.WMObject import WMObject
import WMCore.ACDC.CollectionTypes as CollectionTypes


class Collection(dict, WMObject):
    def __init__(self, **options):
        dict.__init__(self)
        WMObject.__init__(self)
        self.setdefault("name", None)
        self.setdefault("type", CollectionTypes.GenericCollection)
        self.setdefault("filesets", [])
        self.update(options)

    def create(self, unique=False):
        """
        _create_

        Create this Collection in the back end

        """
        pass

    def populate(self):
        """
        _populate_

        Pull in all filesets & file entries

        """
        pass

    def drop(self):
        """
        _drop_

        Remove this collection.
        """
        pass

    def addFileset(self, fileset):
        """
        _addFiles_

        Add a fileset to the collection.
        """
        fileset.setCollection(self)
        self["filesets"].append(fileset)
        return
