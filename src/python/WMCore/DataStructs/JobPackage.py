#!/usr/bin/env python
"""
_JobPackage_

Data structure for storing and retreiving multiple job objects.
"""




import cPickle

from WMCore.DataStructs.WMObject import WMObject

class JobPackage(WMObject, dict):
    """
    _JobPackage_
    """
    def __init__(self):
        dict.__init__(self)

    def save(self, fileName):
        """
        _save_

        Pickle this object and save it to disk.
        """
        fileHandle = open(fileName, "w")
        cPickle.dump(self, fileHandle, -1)
        fileHandle.close()
        return

    def load(self, fileName):
        """
        _load_

        Load a pickled JobPackage object.
        """
        fileHandle = open(fileName, "r")
        loadedJobPackage = cPickle.load(fileHandle)
        self.clear()
        self.update(loadedJobPackage)
        return
