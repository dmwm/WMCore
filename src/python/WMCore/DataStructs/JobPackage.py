#!/usr/bin/env python
"""
_JobPackage_

Data structure for storing and retreiving multiple job objects.
"""
import pickle
from Utils.PythonVersion import HIGHEST_PICKLE_PROTOCOL
from WMCore.DataStructs.WMObject import WMObject


class JobPackage(WMObject, dict):
    """
    _JobPackage_
    """

    def __init__(self, directory=None):
        """
        __init__

        Allow you to set a directory where the
        package will be stored.
        This is then saved for use by the
        JobSubmitter
        """
        dict.__init__(self)
        self.setdefault('directory', directory)

    def save(self, fileName):
        """
        _save_

        Pickle this object and save it to disk.
        """
        with open(fileName, 'wb') as fileHandle:
            pickle.dump(self, fileHandle, protocol=HIGHEST_PICKLE_PROTOCOL)
        return

    def load(self, fileName):
        """
        _load_

        Load a pickled JobPackage object.
        """
        self.clear()
        with open(fileName, 'rb') as fileHandle:
            loadedJobPackage = pickle.load(fileHandle)
        self.update(loadedJobPackage)
        return
