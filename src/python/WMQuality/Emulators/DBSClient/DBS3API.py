"""
_DBS3API_

Mock DBS3 api for testing the DBS3 uploader

Created on Mar 13, 2013

@author: dballest
"""

import json
import os

class DbsApi():
    """
    _DbsApi_

    Replacement DbsApi implementing the required
    functions for the DBS3 upload poller.
    """
    def __init__(self, url):
        """
        _init_

        Store the url where this dbs is "located"
        """
        self.dbsPath = url

    def insertBulkBlock(self, blockDump):
        """
        _insertBulkBlock_

        Insert a block into fake DBS, only insert the block specific information
        and no file information in the file given in the dbsPath
        """

        currentInfo = []
        if os.path.getsize(self.dbsPath):
            inFileHandle = open(self.dbsPath, 'r')
            currentInfo = json.load(inFileHandle)
            inFileHandle.close()
        outFileHandle = open(self.dbsPath, 'w')
        currentInfo.append(blockDump['block'])
        json.dump(currentInfo, outFileHandle)
        outFileHandle.close()

        return
