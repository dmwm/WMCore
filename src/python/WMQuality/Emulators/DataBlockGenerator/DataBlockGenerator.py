from Globals import GlobalParams
import Globals

class DataBlockGenerator(object):
    
    def __init__(self):
        self.blocks = {}        
        self.locations = {}
        self.files = {}
        
    def _dataGenerator(self, dataset):
        
        #some simple process to generate block with consistency
        
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
            
            for fileID in range(numOfFiles):
                if not self.files.has_key(blockName):
                    self.files[blockName] = []
                    
                fileName =  "/store/data/%s/file%s" % (blockName, fileID)
                parentFileName = "/store/data/%s_parent/file%s_parent" % (blockName, fileID)
                dbsFile = {'LogicalFileName': fileName, 
                           'ParentList' : [self.createDBSFile({'LogicalFileName':parentFileName})],
                          }
                self.files[blockName].append(self.createDBSFile(dbsFile))
        
        return self.blocks[dataset]
    
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
            self._dataGenerator(dataset)
        return  self.blocks[dataset]
        
    def getFiles(self, block):
        if not self.files.has_key(block):
            dataset = self.getDataset(block)
            self._dataGenerator(dataset)
        return  self.files[block]
    
    def getLocation(self, block):
        
        return Globals.getSites(block)
    
    def getDataset(self, block):
        return block.split('#')[0]
                
    