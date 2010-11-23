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
    
    def __init__(self):
        self.blocks = {}        
        self.locations = {}
        self.files = {}
    
    def _blockGenerator(self, dataset):
        if dataset.startswith('/' + Globals.NOT_EXIST_DATASET):
            raise NoDatasetError, "no dataset"
        
        self.blocks[dataset] = []
        
        for i in range(GlobalParams.numOfBlocksPerDataset()):
            blockName = "%s#%s" % (dataset, i+1)
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()
            
            self.blocks[dataset].append(
                                    {'Name' : blockName,
                                     'NumberOfEvents' : numOfEvents,
                                     'NumberOfFiles' : numOfFiles,
                                     'NumberOfLumis' : numOfLumis,
                                     'Size' : size,
                                     'Parents' : ()}
                                     )
            
            
    def _fileGenerator(self, blockName):
        
        self.files[blockName] = []
        
        for fileID in range(GlobalParams.numOfFilesPerBlock()):
           
            fileName =  "/store/data%s/file%s" % (blockName, fileID)
            #Not sure why fileName is unit code - change to str
            fileName = str(fileName)
            parentFileName = "/store/data%s_parent/file%s_parent" % (blockName, fileID)
            #Not sure why fileName is unit code - change to str
            parentFileName = str(parentFileName)

            dbsFile = {'LogicalFileName': fileName, 
                       'ParentList' : [self._createDBSFile(blockName,
                                                {'LogicalFileName':parentFileName})],
                      }
            self.files[blockName].append(self._createDBSFile(blockName,
                                                             dbsFile))

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
        
        if not self.blocks.has_key(dataset):
            try:
                self._blockGenerator(dataset)
            except NoDatasetError:
                return []
        return  self.blocks[dataset]
        
    def getFiles(self, block):
        if not self.files.has_key(block):
            self._fileGenerator(block)
        return  self.files[block]
    
    def getLocation(self, block):
        
        return Globals.getSites(block)
    
    def getDatasetName(self, block):
        return block.split('#')[0]
                
    