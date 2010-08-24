from Globals import GlobalParams
import Globals

class DataBlockGenerator(object):
    
    def __init__(self):
        self.blocks = {}        
        self.locations = {}
        self.files = {}
    
    def _blockGenerator(self, dataset):
        
        self.blocks[dataset] = []
        
        for i in range(GlobalParams.numOfBlocksPerDataset()):
            blockName = "%s#%s" % (dataset, i)
            numOfFiles = GlobalParams.numOfFilesPerBlock()
            numOfEvents = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfEventsPerFile()
            size = GlobalParams.numOfFilesPerBlock() * GlobalParams.sizeOfFile()
            
            self.blocks[dataset].append(
                                    {'Name' : blockName,
                                     'NumberOfEvents' : numOfEvents,
                                     'NumberOfFiles' : numOfFiles,
                                     'Size' : size,
                                     'Parents' : ()}
                                     )
            
            
    def _fileGenerator(self, blockName):
        
        self.files[blockName] = []
        
        for fileID in range(GlobalParams.numOfFilesPerBlock()):
           
            fileName =  "/store/data/%s/file%s" % (blockName, fileID)
            parentFileName = "/store/data/%s_parent/file%s_parent" % (blockName, fileID)
            dbsFile = {'LogicalFileName': fileName, 
                       'ParentList' : [self.createDBSFile({'LogicalFileName':parentFileName})],
                      }
            self.files[blockName].append(self.createDBSFile(dbsFile))
    
    def createDBSFile(self, dbsFile = {}):
        defaultDBSFile = {'Checksum': "123456",
                          'NumberOfEvents': GlobalParams.numOfEventsPerFile(),
                          'FileSize': GlobalParams.sizeOfFile(),
                          'ParentList': [],
                          'LumiList': [{'RunNumber': 1, 'LumiSectionNumber': 1}, 
                                       {'RunNumber': 1, 'LumiSectionNumber': 2}]
                          }
        defaultDBSFile.update(dbsFile)
        return defaultDBSFile
        
    def getBlocks(self, dataset):
        
        if not self.blocks.has_key(dataset):
            self._blockGenerator(dataset)
        return  self.blocks[dataset]
        
    def getFiles(self, block):
        if not self.files.has_key(block):
            self._fileGenerator(block)
        return  self.files[block]
    
    def getLocation(self, block):
        
        return Globals.getSites(block)
    
    def getDatasetName(self, block):
        return block.split('#')[0]
                
    