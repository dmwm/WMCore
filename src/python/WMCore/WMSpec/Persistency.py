#!/usr/bin/env python
"""
_PersistencyHelper_

Util class to provide a common persistency layer for ConfigSection derived
objects, with options to save in different formats

Placeholder for ideas at present....

"""

__revision__ = "$Id: Persistency.py,v 1.8 2010/03/05 15:52:08 swakef Exp $"
__version__ = "$Revision: 1.8 $"

import pickle
from urllib2 import urlopen, Request
from urlparse import urlparse

#from WMCore.Wrappers import JsonWrapper
#from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker


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
        #elif extension == 'json':
        #    JsonWrapper.dump(JSONThunker().thunk(self.data), 
        #                     handle)
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

        # urllib2 needs a scheme - assume local file if none given
        if not urlparse(filename)[0]:
            filename = 'file:' + filename
        # Send Accept header so we dont get default which may be fancy ie. json
        handle = urlopen(Request(filename, headers = {"Accept" : "*/*"}))
        extension = filename.split(".")[-1].lower()
        if extension == "pkl":
            self.data = pickle.load(handle)
        #elif extension == 'json':
        #    self.data = JSONThunker().unthunk(JsonWrapper.load(handle))  
        else:
            self.data = pickle.load(handle)
            #self.data = handle.read()
        handle.close()
        return



