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
class MockDBSReader:
    """
    Mock up dbs access
    """
    def __init__(self, url = '', *datasets):
        self.blocks = {}
        args = { "url" : url, "level" : 'ERROR', "version" : ''}
        self.dbs = _MockDBSApi(args)
        for dataset in datasets:
            self.blocks[dataset] = [{'Name' : dataset + "#1",
                                     'NumberOfEvents' : 500,
                                     'NumberOfFiles' : 5,
                                     'NumberOfLumis' : 2, # this is not real dbs property.
                                     'Size' : 100000,
                                     'Parents' : ()},
                                    {'Name' : dataset + "#2",
                                     'NumberOfEvents' : 1000,
                                     'NumberOfFiles' : 10,
                                     'NumberOfLumis' : 2, # this is not real dbs property.
                                     'Size' : 300000,
                                     'Parents' : ()}
                                   ]
            self.locations = {dataset + "#1" : ['SiteA'],
                              dataset + "#2" : ['SiteA', 'SiteB']}

            dbsFile1 = {'Checksum': "12345",
                        'LogicalFileName': "/store/data/fake/file1",
                        'NumberOfEvents': 1000,
                        'FileSize': 102400,
                        'ParentList': [],
                        'LumiList': [{'RunNumber': 1, 'LumiSectionNumber': 1},
                                     {'RunNumber': 1, 'LumiSectionNumber': 2}]
                        }

            dbsFile2 = {'Checksum': "123456",
                        'LogicalFileName': "/store/data/fake_parent/file2",
                        'NumberOfEvents': 1001,
                        'FileSize': 103400,
                        'ParentList': [dbsFile1],
                        'LumiList': [{'RunNumber': 2, 'LumiSectionNumber': 3},
                                     {'RunNumber': 3, 'LumiSectionNumber': 4}]
                        }

            self.files = {dataset + "#1" : [dbsFile1],
                          dataset + "#2" : [dbsFile2]}

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True, blockName = '*'):
        """Fake block info"""
        try:
            if blockName != '*':
                return [x for x in self.blocks[dataset] if x['Name'] == blockName]
            return self.blocks[dataset]
        except KeyError:
            return []

    def listFileBlockLocation(self, block):
        """Fake locations"""
        return self.locations[block]

    def listFilesInBlock(self, block):
        """Fake files"""
        return self.files[block]

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
            for x in self.files[b]:
                results.extend([y['RunNumber'] for y in x['LumiList']])
            return results

        if block:
            return getRunsFromBlock(block)
        if dataset:
            runs = []
            for block in self.blocks[dataset]:
                runs.extend(getRunsFromBlock(block))
            return runs
        return None

    def getDBSSummaryInfo(self, dataset=None, block=None):
        """Dataset/Block summary"""
        result = {}
        if block:
            #TODO: this is hardcoded since addition of file info doesn't
            # match with block info
            if block.endswith('#1'):
                result['NumberOfEvents'] = 500
                result['NumberOfFiles'] = 5
                result['NumberOfLumis'] = 2
                result['path'] = dataset

            if block.endswith('#2'):
                result['NumberOfEvents'] = 1000
                result['NumberOfFiles'] = 10
                result['NumberOfLumis'] = 2
                result['path'] = dataset

            return result

        if dataset:
            result['NumberOfEvents'] = sum([x['NumberOfEvents'] for x in self.blocks[dataset]])
            result['NumberOfFiles'] = sum([x['NumberOfFiles'] for x in self.blocks[dataset]])

            result['NumberOfLumis'] = sum([x['NumberOfLumis'] for x in self.blocks[dataset]])
            result['path'] = dataset

            return result

# pylint: enable-msg=W0613,R0201
