#!/usr/bin/env python
"""
    Mocked DBS interface for Start Policy unit tests
"""

__revision__ = "$Id: MockDBSReader.py,v 1.1 2009/12/14 13:56:40 swakef Exp $"
__version__ = "$Revision: 1.1 $"

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
class MockDBSReader:
    """
    Mock up dbs access
    """
    def __init__(self, url = '', *datasets):
        self.blocks = {}
        self.url = url
        for dataset in datasets:
            self.blocks[dataset] = [{'Name' : dataset + "#1",
                                     'NumEvents' : 500,
                                     'NumFiles' : 5,
                                     'Size' : 100000,
                                     'Parents' : ()},
                                    {'Name' : dataset + "#2",
                                     'NumEvents' : 1000,
                                     'NumFiles' : 10,
                                     'Size' : 300000,
                                     'Parents' : ()}
                                   ]
            self.locations = {dataset + "#1" : ['SiteA'],
                              dataset + "#2" : ['SiteA', 'SiteB']}

            dbsFile1 = {'Checksum': "12345",
                        'LogicalFileName': "/store/data/fake/file1",
                        'NumberOfEvents': 1000,
                        'FileSize': 102400,
                        'ParentList': []
                        }

            dbsFile2 = {'Checksum': "123456",
                        'LogicalFileName': "/store/data/fake_parent/file2",
                        'NumberOfEvents': 1001,
                        'FileSize': 103400,
                        'ParentList': ["/store/data/fake/file1"]
                        }

            self.files = {dataset + "#1" : [dbsFile1],
                          dataset + "#2" : [dbsFile2]}

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True):
        """Fake block info"""
        return self.blocks[dataset]

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

    def getDatasetInfo(self, dataset):
        """Dataset summary"""
        result = {}
        result['number_of_events'] = sum([x['NumEvents'] for x in self.blocks[dataset]])
        result['number_of_files'] = sum([x['NumFiles'] for x in self.blocks[dataset]])
        result['path'] = dataset
        return result
# pylint: enable-msg=W0613,R0201
