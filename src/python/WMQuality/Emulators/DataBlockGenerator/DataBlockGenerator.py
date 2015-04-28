from Globals import GlobalParams
import Globals

class NoDatasetError(StandardError):
    """Standard error baseclass"""
    def __init__(self, error):
        StandardError.__init__(self, error)
        self.msg = 'NoDatasetError'
        self.error = error

class DataBlockGenerator(object):

    def _blockGenerator(self, dataset):
        if dataset.startswith('/' + Globals.NOT_EXIST_DATASET):
            raise NoDatasetError, "no dataset"
        blocks = []
        numOfEvents = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfEventsPerFile()
        for i in range(GlobalParams.numOfBlocksPerDataset()):
            blockName = "%s#%s" % (dataset, i+1)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()

            blocks.append(
                                    {'Name' : blockName,
                                     'NumberOfEvents' : numOfEvents,
                                     'NumberOfFiles' : GlobalParams.numOfFilesPerBlock(),
                                     'NumberOfLumis' : GlobalParams.numOfLumisPerBlock(),
                                     'Size' : size,
                                     'Parents' : (),
                                     'OpenForWriting' : '1' if self._openForWriting() else '0'}
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
                           'PhEDExNodeList' : self.getLocation(blockName),
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
                parentList = [self._createDBSFile(blockName, {'LogicalFileName':parentFileName})]
            else:
                parentList = []
            dbsFile = {'LogicalFileName': fileName,
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
                          # assign run and lumi numbers in appropriate range: note run numbers
                          #  for successive blocks may overlap if numOfRunsPerFile() > 1
                          'LumiList': [{'RunNumber': int(round((1. * lumi * (GlobalParams.numOfRunsPerFile() - 1) / GlobalParams.numOfLumisPerBlock()) + run)),     #random.randint(run, run + GlobalParams.numOfRunsPerFile() -1),
                                        'LumiSectionNumber': run*(GlobalParams.numOfLumisPerBlock()) + lumi -1}
                                       for lumi in range(GlobalParams.numOfLumisPerBlock())]
                          }
        defaultDBSFile.update(dbsFile)
        return defaultDBSFile

    def getBlocks(self, dataset):
        try:
            return self._blockGenerator(dataset)
        except NoDatasetError:
            return []

    def getFiles(self, block, parentFlag = False):
        return self._fileGenerator(block, parentFlag)

    def getLocation(self, block):
        return Globals.getSites(block)

    def getDatasetName(self, block):
        return block.split('#')[0]
