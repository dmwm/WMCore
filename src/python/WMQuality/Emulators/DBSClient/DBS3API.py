"""
_DBS3API_

Mock DBS3 api for testing the DBS3 uploader

Created on Mar 13, 2013

@author: dballest
"""

import json
import os

from random import random

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
        and no file information in the file given in the dbsPath.
        There is 20% chance of a proxy error!
        """

        currentInfo = []
        if os.path.getsize(self.dbsPath):
            with open(self.dbsPath, 'r') as inFileHandle:
                currentInfo = json.load(inFileHandle)
        with open(self.dbsPath, 'w') as outFileHandle:
            for block in currentInfo:
                if block["block"]["block_name"] == blockDump["block"]["block_name"]:
                    raise Exception("Block %s already exists" % blockDump["block"]["block_name"])
            currentInfo.append(blockDump)
            json.dump(currentInfo, outFileHandle)

        randomNumber = random()
        if randomNumber < 0.2:
            raise Exception("Proxy Error, this is a mock proxy error.")

        return

    def listBlocks(self, block_name):
        """
        _listBlocks_

        Return the requested block information if it exists.
        """
        if os.path.getsize(self.dbsPath):
            with open(self.dbsPath, 'r') as inFileHandle:
                currentInfo = json.load(inFileHandle)
                inFileHandle.close()
                for block in currentInfo:
                    if block["block"]["block_name"] == block_name:
                        return [block["block"]]
        return []
