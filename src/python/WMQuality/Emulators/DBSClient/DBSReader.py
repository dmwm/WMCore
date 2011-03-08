#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""

from DBSAPI.dbsApi import DbsApi

class _MockDBSApi():
    """Mock dbs api"""
    def __init__(self, args):
        # just make sure args value complies with dbs args
        DbsApi(args)
        self.args = args

    def getServerUrl(self):
        return self.args.get('url', '')

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
    def __init__(self, url, **contact):
        self.dataBlocks = DataBlockGenerator()
        args = { "url" : url, "level" : 'ERROR', "version" : 'DBS_2_0_9'}
        self.dbs = _MockDBSApi(args)
        
    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True,
                          blockName = '*', locations = True):

        """Fake block info"""
        blocks = [x for x in self.dataBlocks.getBlocks(dataset)
                if x['Name'] == blockName or blockName == '*']
        if locations:
            [x.__setitem__('StorageElementList',
                           self.listFileBlockLocation(x['Name'])) for x in blocks]
        return blocks

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

    def listRuns(self, dataset = None, block = None):
        def getRunsFromBlock(b):
            results = []
            for x in self.dataBlocks.getFiles(b):
                results.extend([y['RunNumber'] for y in x['LumiList']])
            return results

        if block:
            return getRunsFromBlock(block)
        if dataset:
            runs = []
            for block in self.dataBlocks.getBlocks(dataset):
                runs.extend(getRunsFromBlock(block))
            return runs
        return None


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

            result['NumberOfLumis'] = len(getLumisectionsInBlock(block))

            result['path'] = dataset

        if dataset:
            if self.dataBlocks.getBlocks(dataset):
                result['NumberOfEvents'] = sum([x['NumberOfEvents']
                                    for x in self.dataBlocks.getBlocks(dataset)])
                result['NumberOfFiles'] = sum([x['NumberOfFiles']
                                    for x in self.dataBlocks.getBlocks(dataset)])
                lumis = set()
                for b in self.dataBlocks.getBlocks(dataset):
                    lumis = lumis.union(getLumisectionsInBlock(b['Name']))

                result['NumberOfLumis'] = len(lumis)
                result['path'] = dataset

        return result

# pylint: enable-msg=W0613,R0201
