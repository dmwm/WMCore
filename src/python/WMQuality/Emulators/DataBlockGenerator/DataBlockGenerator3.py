from __future__ import absolute_import
from .Globals import GlobalParams
from . import Globals

class NoDatasetError(Exception):
    """Standard error baseclass"""
    def __init__(self, error):
        Exception.__init__(self, error)
        self.msg = 'NoDatasetError'
        self.error = error

class DataBlockGenerator3(object):

    def _blockGenerator(self, dataset):
        if dataset.startswith('/' + Globals.NOT_EXIST_DATASET):
            raise NoDatasetError, "no dataset"
        blocks = []
        numOfEvents = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfEventsPerFile()
        for i in range(GlobalParams.numOfBlocksPerDataset()):
            blockName = "%s#%s" % (dataset, i+1)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()

            blocks.append(
                                    {'block_name' : blockName,
                                     'NumberOfEvents' : numOfEvents,
                                     'NumberOfFiles' : GlobalParams.numOfFilesPerBlock(),
                                     'NumberOfLumis' : GlobalParams.numOfLumisPerBlock(),
                                     'file_count' : GlobalParams.numOfFilesPerBlock(),
                                     'Size' : size,
                                     'open_for_writing' : '1' if self._openForWriting() else '0'}
                                     )
        return blocks

    def _openForWriting(self):
        """Is block open or closed?
        Should do this on a block by block basis but so far not needed,
        just make a global state"""
        return GlobalParams.blocksOpenForWriting()

    def getParentBlock(self, block, numberOfParents = 1):
        blocks = []
        numOfEvents = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfEventsPerFile()
        for i in range(numberOfParents):
            dataset, blockname = block.split('#') # append parent block id to tier
            blockName = "%s_parent_%s#%s" % (dataset, i+1, blockname)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()

            blocks.append({'Name' : blockName,
                           'NumberOfEvents' : numOfEvents,
                           'NumberOfFiles' : GlobalParams.numOfFilesPerBlock(),
                           'NumberOfLumis' : GlobalParams.numOfLumisPerBlock(),
                           'Size' : size,
                           'StorageElementList' : self.getLocation(blockName),
                           'Parents' : ()}
                           )
        return blocks

    def _fileGenerator(self, blockName, parentFlag):

        files = []

        for fileID in range(GlobalParams.numOfFilesPerBlock()):

            fileName =  "/store/data%s/file%s" % (blockName, fileID)
            #Not sure why fileName is unit code - change to str
            fileName = str(fileName)
            parentFileName = "/store/data%s_parent/file%s_parent" % (blockName, fileID)
            #Not sure why fileName is unit code - change to str
            parentFileName = str(parentFileName)

            if parentFlag:
                parentList = [self._createDBSFile(blockName, {'logical_file_name':parentFileName})]
            else:
                parentList = []
            dbsFile = {'logical_file_name': fileName,
                       'ParentList' : parentList,
                      }
            files.append(self._createDBSFile(blockName, dbsFile))
        return files

    def _createDBSFile(self, blockName, dbsFile = {}):
        run =  GlobalParams.getRunNumberForBlock(blockName)
        defaultDBSFile = {'Checksum': "123456",
                          'NumberOfEvents': GlobalParams.numOfEventsPerFile(),
                          'FileSize': GlobalParams.sizeOfFile(),
                          'ParentList': [],
                         }
        lumiList = []

        for run in xrange(1, GlobalParams.numOfRunsPerFile() + 1):
            lumiList.append(
                {run:[run*(GlobalParams.numOfLumisPerBlock()) + lumi -1
                        for lumi in range(GlobalParams.numOfLumisPerBlock())]
                })

        defaultDBSFile.update({'LumiList':lumiList})
        defaultDBSFile.update(dbsFile)

        return defaultDBSFile

    def getBlocks(self, dataset):
        try:
            return self._blockGenerator(dataset)
        except NoDatasetError:
            return []

    def getFiles(self, block, parentFlag = False):
        return self._fileGenerator(block, parentFlag)

    def getFile(self, parentFlag = False):
        return self._createDBSFile('dummy#1', parentFlag)

    def getFileLumis(self, block):
        return self._fileLumiGenerator(block)

    def getLocation(self, block):
        return Globals.getSites(block)

    def getDatasetName(self, block):
        return block.split('#')[0]
