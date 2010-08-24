#!/usr/bin/env python
"""
_PersistencyHelper_

Util class to provide a common persistency layer for ConfigSection derived
objects, with options to save in different formats

Placeholder for ideas at present....

"""

import pickle




class PersistencyHelper:
    """
    _PersistencyHelper_

    Save a WMSpec object to a file using pickle

    Future ideas:
    - pickle mode: read/write using pickle
    - python mode: write using pythonise, read using import
       Needs work to preserve tree information
    - gzip mode: also gzip/unzip content if set to True


    """
    def save(self, filename):
        """
        _save_

        Pickle data to a file

        """
        handle = open(filename, 'w')
        pickle.dump(self.data, handle)
        handle.close()
        return

    def load(self, filename):
        """
        _load_

        Unpickle data from file

        """
        handle = open(filename, 'r')
        self.data = pickle.load(handle)
        handle.close()
        return




