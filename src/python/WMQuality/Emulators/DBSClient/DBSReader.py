#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
    This emulates DBS2 which is dead anyhow and should be removed. Of course that will blow up lots of tests.
"""

from WMCore.Services.DBS.DBSErrors import DBSReaderError

class _MockDBSApi():
    """Mock dbs api"""
    def __init__(self, args):
        # just make sure args value complies with dbs args
        try:
            from dbs.apis.dbsClient import DbsApi
            DbsApi(args)
        except ImportError:
            # No dbsApi available, carry on
            pass
        self.args = args

    def getServerUrl(self):
        return self.args.get('url', '')

    def getServerInfo(self):
        """getServerInfo"""
        return {'InstanceName' : 'GLOBAL'}

    def listFiles(self, datasetPath, retriveList):
        res = []
        dbg = DataBlockGenerator()
        for block in dbg.getBlocks(datasetPath):
            files = dbg.getFiles(block['Name'])
            for f in files:
                f['Block'] = block
                res.append(f)

        return res

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable=W0613,R0201
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
        if not blocks:
            # Weird error handling follows, this is what dbs does:
            # If block specified, return [], else raise DbsBadRequest error
            if blockName != '*':
                return []
            else:
                raise DBSReaderError('DbsBadRequest: DBS Server Raised An Error')
        if locations:
            for block in blocks:
                block['PhEDExNodeList'] = [{'Role' : '', 'Name' : x} for x in \
                                               self.listFileBlockLocation(block['Name'])]
        return blocks

    def lfnsInBlock(self, fileBlockName):
        """
        _lfnsInBlock_
        Get a fake list of LFNs for the block
        """

        files = self.listFilesInBlock(fileBlockName)

        return [x['LogicalFileName'] for x in files]

    def listFileBlocks(self, dataset, onlyClosedBlocks = False,
                       blockName = '*'):
        """Get fake block names"""
        return [x['Name'] for x in self.getFileBlocksInfo(dataset, onlyClosedBlocks = False,
                                                          blockName = blockName,
                                                          locations = False)]

    def listOpenFileBlocks(self, dataset):
        """
        _listOpenFileBlocks_

        Retrieve a list of open fileblock names for a dataset

        """
        return [x['Name'] for x in self.getFileBlocksInfo(dataset, onlyClosedBlocks = False,
                                                          locations = False) if str(x['OpenForWriting' ]) == '1']

    def listFileBlockLocation(self, block):
        """Fake locations"""
        return self.dataBlocks.getLocation(block)

    def listFilesInBlock(self, fileBlockName):
        """Fake files"""
        return self.dataBlocks.getFiles(fileBlockName)

    def listFilesInBlockWithParents(self, block):
        return self.dataBlocks.getFiles(block, True)

    def getFileBlock(self, block):
        """Return block + locations"""
        result = { block : {
            "PhEDExNodeNames" : self.listFileBlockLocation(block),
            "Files" : self.listFilesInBlock(block),
            "IsOpen" : self.dataBlocks._openForWriting(),
            }
                }
        return result

    def getFileBlockWithParents(self, fileBlockName):
        """
        _getFileBlockWithParents_

        return a dictionary:
        { blockName: {
             "PhEDExNodeNames" : [<pnn list>],
             "Files" : dictionaries representing each file
             }
        }

        files

        """

        result = { fileBlockName: {
            "PhEDExNodeNames" : self.listFileBlockLocation(fileBlockName),
            "Files" : self.listFilesInBlockWithParents(fileBlockName),
            "IsOpen" : self.dataBlocks._openForWriting(),

            }
                   }
        return result

    def listRuns(self, dataset = None, block = None):
        def getRunsFromBlock(b):
            results = set()
            for x in self.dataBlocks.getFiles(b):
                results = results.union([y['RunNumber'] for y in x['LumiList']])
            return list(results)

        if block:
            return getRunsFromBlock(block)
        if dataset:
            runs = set()
            for block in self.dataBlocks.getBlocks(dataset):
                runs = runs.union(getRunsFromBlock(block['Name']))
            return list(runs)
        return None

    def listRunLumis(self, dataset = None, block = None):
        def getRunsFromBlock(b):
            results = {}
            for x in self.dataBlocks.getFiles(b):
                for y in x['LumiList']:
                    if y['RunNumber'] not in results:
                        results[y['RunNumber']] = 0
                    results[y['RunNumber']] = None  # To match DBS3
            return results

        if block:
            return getRunsFromBlock(block)
        if dataset:
            runs = {}
            for block in self.dataBlocks.getBlocks(dataset):
                updateRuns = getRunsFromBlock(block['Name'])
                for run in updateRuns:
                    if run not in runs:
                        runs[run] = 0
                    runs[run] = None  # To match DBS3
            return runs
        return None


    def getDBSSummaryInfo(self, dataset=None, block=None):
        """Dataset summary"""

        def getLumisectionsInBlock(b):
            lumis = 0
            for file in self.dataBlocks.getFiles(b):
                for x in file['LumiList']:
                    lumis =+ len(x['LumiSectionNumber'])
            return lumis

        result = {}
        if block:
            result['NumberOfEvents'] = str(sum([x['NumberOfEvents']
                                for x in self.dataBlocks.getFiles(block)]))
            result['NumberOfFiles'] = str(len(self.dataBlocks.getFiles(block)))

            result['NumberOfLumis'] = str(getLumisectionsInBlock(block))

            result['path'] = dataset
            result['block'] = block
            result['OpenForWriting'] = '1' if self.dataBlocks._openForWriting() else '0'

        if dataset:
            if self.dataBlocks.getBlocks(dataset):
                result['NumberOfEvents'] = str(sum([x['NumberOfEvents']
                                    for x in self.dataBlocks.getBlocks(dataset)]))
                result['NumberOfFiles'] = str(sum([x['NumberOfFiles']
                                    for x in self.dataBlocks.getBlocks(dataset)]))
                lumis = 0
                for b in self.dataBlocks.getBlocks(dataset):
                    lumis += b['NumberOfLumis']

                result['NumberOfLumis'] = str(lumis)
                result['path'] = dataset

        # Weird error handling follows, this is what dbs does
        if not result:
            raise DBSReaderError('DbsConnectionError: Database exception,Invalid parameters')
        return result

    def listBlockParents(self, block):
        return self.dataBlocks.getParentBlock(block, 1)

    def listDatasetLocation(self, dataset):
        """
        _listDatasetLocation_

        List the SEs where there is at least a block of the given
        dataset.
        """
        blocks = self.getFileBlocksInfo(dataset, onlyClosedBlocks = False,
                                        blockName = '*', locations = True)

        result = set()
        for block in blocks:
            result |= set([x['Name'] for x in block['PhEDExNodeList']])

        return list(result)

# pylint: enable=W0613,R0201
