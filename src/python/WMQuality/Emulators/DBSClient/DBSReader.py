#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""

class _MockDBSApi():
    """Mock dbs api"""
    def __init__(self, args):
        self.args = args
        self.url = args.get('url', '')

    def getServerInfo(self):
        """getServerInfo"""
        return {'InstanceName' : 'GLOBAL'}


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
        url = args[0]
        args = { "url" : url, "level" : 'ERROR', "version" : ''}
        self.dbs = _MockDBSApi(args)
        
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

    def getDBSSummaryInfo(self, dataset=None, block=None):

        """Dataset summary"""
        def getLumisectionsInBlock(b):
            lumis = set()
            for file in self.dataBlocks.getFiles(b):
                for x in file['LumiList']:
                    lumis.add(x['LumiSectionNumber'])
            return lumis

        result = {}
        if block:
            result['NumberOfEvents'] = sum([x['NumberOfEvents']
                                for x in self.dataBlocks.getFiles(block)])
            result['NumberOfFiles'] = len(self.dataBlocks.getFiles(block))

            result['NumberOfLumis'] = sum(getLumisectionsInBlock(block))

            result['path'] = dataset

        if dataset:
            result['NumberOfEvents'] = sum([x['NumberOfEvents']
                                for x in self.dataBlocks.getBlocks(dataset)])
            result['NumberOfFiles'] = sum([x['NumberOfFiles']
                                for x in self.dataBlocks.getBlocks(dataset)])
            lumis = set()
            for b in self.dataBlocks.getBlocks(dataset):
                lumis.union(getLumisectionsInBlock(b['Name']))

            result['NumberOfLumis'] = sum(lumis)
            result['path'] = dataset
        return result

# pylint: enable-msg=W0613,R0201
