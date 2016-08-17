#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""

from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.DBS.DBS3Reader import remapDBS3Keys

class _MockDBSApi():
    """Mock dbs api"""
    def __init__(self, args, **contact):
        # just make sure args value complies with dbs args
        try:
            from dbs.apis.dbsClient import DbsApi
            DbsApi(args, **contact)
        except ImportError:
            # No dbsApi available, carry on
            pass
        self.args = args
        self.dbg = DataBlockGenerator3()

    def listFileArray(self, dataset = None, block_name = None, run_num = None, lumi_list = [], detail = False):
        raise NotImplementedError

    def listFileLumis(self, logical_file_name = None, block_name = None, validFileOnly = 1):
        raise NotImplementedError

    def listFileSummaries(self, block_name = None, dataset = None, run_num = None, validFileOnly = 1):
        raise NotImplementedError


from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator3 import DataBlockGenerator3

class DBS3Reader:
    """
    Mock up dbs access
    """
    def __init__(self, url, **contact):
        self.dataBlocks = DataBlockGenerator3()
        self.dbs = _MockDBSApi(url, **contact)

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True,
                          blockName = '*', locations = True):
        raise NotImplementedError

    def lfnsInBlock(self, fileBlockName):
        raise NotImplementedError

    def listFileBlocks(self, dataset, onlyClosedBlocks = False,
                       blockName = '*'):
        raise NotImplementedError

    def listOpenFileBlocks(self, dataset):
        raise NotImplementedError

    def listFileBlockLocation(self, block):
        raise NotImplementedError

    def listFilesInBlock(self, fileBlockName):
        raise NotImplementedError

    def listFilesInBlockWithParents(self, block):
        raise NotImplementedError

    def getFileBlock(self, block):
        raise NotImplementedError

    def getFileBlockWithParents(self, fileBlockName):
        raise NotImplementedError

    def listRuns(self, dataset = None, block = None):
        def getRunsFromBlock(b):
            results = set()
            for x in self.dataBlocks.getFiles(b):
                results = results.union([y['RunNumber'] for y in x['LumiList']])
            return list(results)

        raise NotImplementedError

    def listRunLumis(self, dataset = None, block = None):
        def getRunsFromBlock(b):
            results = {}
            for x in self.dataBlocks.getFiles(b):
                for y in x['LumiList']:
                    if y['RunNumber'] not in results:
                        results[y['RunNumber']] = 0
                    results[y['RunNumber']] += 1
            return results

        raise NotImplementedError



        #def getDBSSummaryInfo(self, dataset=None, block=None):

        #"""Dataset summary"""
        #def getLumisectionsInBlock(b):
            #lumis = set()
            #for file in self.dataBlocks.getFiles(b):
                #pdb.set_trace()
                #for x in file['LumiList']:
                    #lumis.add(x['LumiSectionNumber'])
            #return lumis

        #result = {}
        #if block:
            #result['NumberOfEvents'] = str(sum([x['NumberOfEvents']
                                #for x in self.dataBlocks.getFiles(block)]))
            #result['NumberOfFiles'] = str(len(self.dataBlocks.getFiles(block)))

            #result['NumberOfLumis'] = 0 #FIXME? str(len(getLumisectionsInBlock(block)))

            #result['path'] = dataset
            #result['block'] = block
            #result['OpenForWriting'] = '1' if self.dataBlocks._openForWriting() else '0'

        #if dataset:
            #if self.dataBlocks.getBlocks(dataset):
                #result['NumberOfEvents'] = str(sum([x['NumberOfEvents']
                                    #for x in self.dataBlocks.getBlocks(dataset)]))
                #result['NumberOfFiles'] = str(sum([x['NumberOfFiles']
                                    #for x in self.dataBlocks.getBlocks(dataset)]))
                #lumis = set()
                #for b in self.dataBlocks.getBlocks(dataset):
                    #lumis = lumis.union(getLumisectionsInBlock(b['block_name']))

                #result['NumberOfLumis'] = str(len(lumis))
                #result['path'] = dataset

        ## Weird error handling follows, this is what dbs does
        #if not result:
            #raise DBSReaderError('DbsConnectionError: Database exception,Invalid parameters')
        #return result

    def getDBSSummaryInfo(self, dataset = None, block = None):
        """
        Get dataset summary includes # of files, events, blocks and total size
        """

        raise NotImplementedError

    def listBlockParents(self, block):
        raise NotImplementedError

    def listDatasetLocation(self, dataset):
        raise NotImplementedError

    def getFileListByDataset(self, dataset, detail=True):
        raise NotImplementedError

# pylint: enable=W0613,R0201
