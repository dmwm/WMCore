#!/usr/bin/env python
"""
_JobPackage_

Data structure for storing and retreiving multiple job objects.
"""

__revision__ = "$Id: JobPackage.py,v 1.1 2009/06/08 20:01:45 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import cPickle

from WMCore.DataStructs.WMObject import WMObject

class JobPackage(WMObject, list):
    """
    _JobPackage_
    """
    def __init__(self):
        list.__init__(self)

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
        del self[0:len(self)]
        self.extend(loadedJobPackage)
        return
