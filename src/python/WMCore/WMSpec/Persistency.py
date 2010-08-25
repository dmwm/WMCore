#!/usr/bin/env python
"""
_PersistencyHelper_

Util class to provide a common persistency layer for ConfigSection derived
objects, with options to save in different formats

Placeholder for ideas at present....

"""

import pickle
from urllib import urlopen
from WMCore.Wrappers import JsonWrapper

class PersistencyHelper:
    """
    _PersistencyHelper_

    Save a WMSpec object to a file using pickle

    Future ideas:
    - pickle mode: read/write using pickle
    - python mode: write using pythonise, read using import
       Needs work to preserve tree information
    - gzip mode: also gzip/unzip content if set to True
    - json mode: read/write using json

    """
        
    def save(self, filename):
        """
        _save_

        Save data to a file
        Saved format is defined depending on the extension
        """
        handle = open(filename, 'w')
        extension = filename.split(".")[-1].lower()
        if extension == "pkl":
            pickle.dump(self.data, handle)
        elif extension == 'json':
            JsonWrapper.dump(self.data, handle)
        else:
            pickle.dump(self.data, handle)
            #handle.write(str(self.data))
            
        handle.close()
        return

    def load(self, filename):
        """
        _load_

        Unpickle data from file

        """
        
        #TODO: currently support both loading from file path or url
        #if there are more things to filter may be separate the load function
        handle = urlopen(filename)
        extension = filename.split(".")[-1].lower()
        if extension == "pkl":
            self.data = pickle.load(handle)
        elif extension == 'json':
            self.data = JsonWrapper.load(handle)
        else:
            self.data = pickle.load(handle)
            #self.data = handle.read()
        handle.close()
        return



