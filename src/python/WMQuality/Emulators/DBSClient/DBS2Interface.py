"""
_DBS2Interface_

Module containing a mock class to run unittests on the DBS2 uploader

Created on Mar 12, 2013

@author: dballest
"""

from WMComponent.DBSUpload.DBSErrors import DBSInterfaceError

class DBS2Interface():
    """
    _DBS2Interface_

    Provide a mock interface for DBS2 that can be used to test the full DBS2 uploader
    without the actual upload or the need of a real DBS2 instance.
    """
    def __init__(self, config):
        """
        __init__

        Initialize the storage of fake DBS data
        """
        self.dbs = 'FakeButNotNone'
        self.algoList = []
        self.processedDatasetList = []
        self.primaryDatasetList = []
        self.blocks = {}

    def runDBSBuffer(self, algo, dataset, blocks, override = False):
        """
        _runDBSBuffer_

        Run the entire fake DBSBuffer chain
        """
        # First create the dataset
        processed = self.insertDatasetAlgo(algo = algo, dataset = dataset,
                                           override = override)

        # Next create blocks
        affBlocks = self.createAndInsertBlocks(dataset = dataset,
                                               procDataset = processed,
                                               blocks = blocks)

        return affBlocks

    def insertDatasetAlgo(self, algo, dataset, override = False):
        """
        _insertDatasetAlgo_

        Insert a dataset-algo combination in fake DBS2
        """
        dbsRef = None
        if override or not algo['InDBS']:
            # Then put the algo in DBS by referencing local DBS
            dbsRef = self.dbs

        # Create a DBS Algo
        dbsAlgo = self._createAlgorithm(apiRef = dbsRef,
                                          appName = algo['ApplicationName'],
                                          appVer = algo['ApplicationVersion'],
                                          appFam = algo['ApplicationFamily'],
                                          PSetHash = algo['PSetHash'],
                                          PSetContent = algo['PSetContent'])

        if dataset['PrimaryDataset'].lower() == 'bogus':
            # Do not commit bogus datasets!
            return None

        dbsRef = self.dbs
        if dataset.get('DASInDBS', None):
            # Then this whole thing is already in DBS
            dbsRef = None

        primary = self._createPrimaryDataset(apiRef = dbsRef,
                                             primaryName = dataset['PrimaryDataset'])

        processed = self._createProcessedDataset(apiRef = dbsRef,
                                                 algorithm = dbsAlgo,
                                                 primary = primary,
                                                 processedName = dataset['ProcessedDataset'],
                                                 dataTier = dataset['DataTier'],
                                                 status = dataset['status'],
                                                 globalTag = dataset['globalTag'],
                                                 parent = dataset['parent'])

        return processed

    def createAndInsertBlocks(self, dataset, procDataset, blocks):
        """
        _createAndInsertBlocks_

        Create all the blocks, account for the number of files, events and size only.
        """
        affectedBlocks = []

        for block in blocks:
            # Create each block one at a time and insert its files

            # First create the block
            self._createUncheckedBlock(apiRef = self.dbs, name = block['Name'],
                                       datasetPath = dataset['Path'],
                                       seName = block['location'])

            block['Path'] = dataset['Path']
            block['StorageElementList'] = block['location']

            # Now assemble the files
            block['readyFiles'] = block['newFiles']
            flag = False
            if block['open'] == 'Pending':
                flag = True

            finBlock = self.insertFilesAndCloseBlocks(block = block, close = flag)
            affectedBlocks.append(finBlock)
        return affectedBlocks

    def insertFilesAndCloseBlocks(self, block, close = False):
        """
        _insertFilesAndCloseBlocks_

        Insert files into blocks and close them.
        This does all the actual work of closing a block, first
        inserting the file info, then actually closing if you have
        toggled the 'close' flag. All in the fake DBS.
        """
        # Insert all the files added to the block in this round
        blockRef = self.blocks[block['Name']]
        for fileInfo in block.get('readyFiles', []):
            blockRef['events'] += fileInfo['events']
            blockRef['size'] += fileInfo['size']
            blockRef['nFiles'] += 1

        # Close the block if requested
        if close:
            blockRef['OpenForWriting'] = '0'
            block['OpenForWriting'] = '0'
            block['open'] = 0

        return block

    def _createAlgorithm(self, apiRef, appName, appVer, appFam,
                         PSetHash = None, PSetContent = None):
        """
        _createAlgorithm_

        Create an algo object in fake DBS2
        """
        algoObject = {'exe' : appName,
                      'version' : appVer,
                      'family' : appFam,
                      'hash' : PSetHash,
                      'content' : PSetContent}
        if apiRef:
            self.algoList.append(algoObject)
        return algoObject

    def _createPrimaryDataset(self, primaryName, primaryDatasetType = 'mc', apiRef = None):
        """
        _createPrimaryDataset_

        Create an primds object in fake DBS2
        """
        primDsObject = {'name' : primaryName,
                        'type' : primaryDatasetType}
        if apiRef:
            self.primaryDatasetList.append(primDsObject)
        return primDsObject

    def _createProcessedDataset(self, algorithm, apiRef, primary, processedName, dataTier,
                                group = "NoGroup", status = "VALID",
                                globalTag = '', parent = None):
        """
        _createProcessedDataset_

        Create an procds object in fake DBS2
        """
        if algorithm not in self.algoList:
            raise DBSInterfaceError("No algo object inserted before associated processed dataset")
        if primary not in self.primaryDatasetList:
            raise DBSInterfaceError("No primds object inserted before associated processed dataset")
        procDsObject = {'name' : processedName,
                        'tier' : dataTier,
                        'group' : group,
                        'status' : status,
                        'tag' : globalTag,
                        'parent' : parent}
        if apiRef:
            self.processedDatasetList.append(procDsObject)
        return procDsObject

    def _createUncheckedBlock(self, apiRef, name, datasetPath, seName):
        """
        _createUncheckedBlock_

        Create blocks object if not existent and store them in the DBSInterface memory
        """
        blockObject = {'name' : name,
                       'datasetPath' : datasetPath,
                       'storage_elements' : [seName],
                       'events' : 0,
                       'nFiles' : 0,
                       'size' : 0,
                       'OpenForWriting' : '1'}
        if name not in self.blocks:
            self.blocks[name] = blockObject

        return self.blocks[name]
