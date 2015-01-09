#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""

from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.DBS.DBS3Reader import remapDBS3Keys

import pdb

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
        res = []

        if dataset:
            for block in self.dbg.getBlocks(dataset):
                files = self.dbg.getFiles(block['block_name'])
                for f in files:
                    f['block_name'] = block['block_name']
                    res.append(f)
        elif block_name:
            files = self.dbg.getFiles(block_name)
            for f in files:
                f['block_name'] = block_name
                res.append(f)

        return res

    def listFileLumis(self, logical_file_name = None, block_name = None, validFileOnly = 1):
        """
        Mock out listFileLumis
        """
        results = []

        if block_name:
            for file in self.dbg.getFiles(block_name):
                for runLumis in file['LumiList']:
                    for run, lumis in runLumis.items():
                        results.append({
                            'logical_file_name' : file['logical_file_name'],
                            'run_num' : run,
                            'lumi_section_num' : lumis,
                        })

        if logical_file_name:
            file = self.dbg.getFile()
            for runLumis in file['LumiList']:
                for run, lumis in runLumis.items():
                    results.append({
                        'logical_file_name' : file['logical_file_name'],
                        'run_num' : run,
                        'lumi_section_num' : lumis,
                    })

        return results

    def listFileSummaries(self, block_name = None, dataset = None, run_num = None, validFileOnly = 1):
        """
        Mock out listFileSummaries

        API to list number of files, event counts and number of lumis in a given block or dataset. If the optional run parameter
        is used, the summary is just for this run number. Either block_name or dataset name is required. No wild-cards are allowed

            Parameters:
            block_name (str) ? Block name
            dataset (str) ? Dataset name
            run_num (int, str, list) ? Run number (Optional)
            Returns:
            List of dictionaries containing the following keys (num_files, num_lumi, num_block, num_event, file_size)

        """

        if dataset or run_num:
            raise NotImplementedError

        if block_name:
            files = self.listFileArray(block_name = block_name)
            summary = {'num_files' : 0, 'num_lumi' : 0, 'num_block' : 1, 'num_event' : 0, 'file_size' : 0}
            for file in files:
                summary['num_files'] += 1
                summary['num_event'] += file['NumberOfEvents']
                summary['file_size'] += file['FileSize']
                for runLumis in file['LumiList']:
                    for run, lumis in runLumis.items():
                        summary['num_lumi'] += len(lumis)

        return [summary]

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

        """Fake block info"""
        blocks = [x for x in self.dataBlocks.getBlocks(dataset)
                if x['block_name'] == blockName or blockName == '*']
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
        return [x['block_name'] for x in self.getFileBlocksInfo(dataset, onlyClosedBlocks = False,
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
                    results[y['RunNumber']] += 1
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
                    runs[run] += updateRuns[run]
            return runs
        return None



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

        try:
            if block:
                summary = self.dbs.listFileSummaries(block_name = block)
            else: # dataset case dataset shouldn't be None
                summary = self.dbs.listFileSummaries(dataset = dataset)
        except DBSReaderError as ex:
            msg = "Error in DBSReader.listDatasetSummary(%s, %s)\n" % (dataset, block)
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        if not summary or summary[0].get('file_size') is None: # appears to indicate missing dataset
            msg = "DBSReader.listDatasetSummary(%s, %s): No matching data"
            raise DBSReaderError(msg % (dataset, block))
        result = remapDBS3Keys(summary[0], stringify = True)
        result['path'] = dataset if not block else ''
        result['block'] = block if block else ''
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
