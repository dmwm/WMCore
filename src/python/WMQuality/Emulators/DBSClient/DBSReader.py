#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""




#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

class DBSReader:
    """
    Mock up dbs access
    """
    def __init__(self, *args, **kwargs):
        print "Using DBS Emulator ..."
        self.dataBlocks = DataBlockGenerator()
        
    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True):
        """Fake block info"""
        return self.dataBlocks.getBlocks(dataset)

    def listFileBlockLocation(self, block):
        """Fake locations"""
        return self.dataBlocks.getLocation(block)

    def listFilesInBlock(self, block):
        """Fake files"""
        return self.dataBlocks.getFiles(block)

    def getFileBlock(self, block):
        """Return block + locations"""
        result = { block : {
            "StorageElements" : self.listFileBlockLocation(block),
            "Files" : self.listFilesInBlock(block),
            "IsOpen" : False,
            }
                   }
        return result

    def getDatasetInfo(self, dataset):
        """Dataset summary"""
        result = {}
        result['NumberOfEvents'] = sum([x['NumberOfEvents'] 
                                for x in self.dataBlocks.getBlocks(dataset)])
        result['NumberOfFiles'] = sum([x['NumberOfFiles'] 
                                for x in self.dataBlocks.getBlocks(dataset)])
        result['path'] = dataset
        return result
# pylint: enable-msg=W0613,R0201
