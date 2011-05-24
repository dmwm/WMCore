from Globals import GlobalParams
import Globals

class NoDatasetError(StandardError):
    """Standard error baseclass"""
    def __init__(self, error):
        StandardError.__init__(self, error)
        self.msg = 'NoDatasetError'
        self.error = error

numOfFiles = GlobalParams.numOfFilesPerBlock()
numOfEvents = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfEventsPerFile()
numOfLumis = GlobalParams.numOfLumisPerBlock()

class DataBlockGenerator(object):
    
    def _blockGenerator(self, dataset):
        if dataset.startswith('/' + Globals.NOT_EXIST_DATASET):
            raise NoDatasetError, "no dataset"
        blocks = []
        for i in range(GlobalParams.numOfBlocksPerDataset()):
            blockName = "%s#%s" % (dataset, i+1)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()
            
            blocks.append(
                                    {'Name' : blockName,
                                     'NumberOfEvents' : numOfEvents,
                                     'NumberOfFiles' : numOfFiles,
                                     'NumberOfLumis' : numOfLumis,
                                     'Size' : size,
                                     'Parents' : ()}
                                     )
        return blocks

    def getParentBlock(self, block, numberOfParents = 1):
        blocks = []
        for i in range(numberOfParents):
            blockName = "%s_parent_%s" % (block, i+1)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()
            
            blocks.append({'Name' : blockName,
                           'NumberOfEvents' : numOfEvents,
                           'NumberOfFiles' : numOfFiles,
                           'NumberOfLumis' : numOfLumis,
                           'Size' : size,
                           'StorageElementList':[{'Role' : '', 'Name' : x} for x in \
                                               self.getLocation(blockName)],
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
                          'LumiList': [{'RunNumber': run,
                                        'LumiSectionNumber': run*(numOfLumis) + lumi -1}
                                       for lumi in range(numOfLumis)]
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
                
