import Globals

class DataBlockGenerator(object):
    
    def __init__(self):
        self.blocks = {}        
        self.locations = {}
        self.files = {}
        
    def _dataGenerator(self, dataset):
        
        #some simple process to generate block with consistency
        
        self.blocks[dataset] = []
        
        for i in range(Globals.NUM_OF_BLOCKS_PER_DATASET):
            blockName = "%s#%s" % (dataset, i)
            numOfFiles = Globals.NUM_OF_FILES_PER_BLOCK
            numOfEvents = Globals.NUM_OF_FILES_PER_BLOCK * Globals.NUM_OF_EVENTS_PER_FILE
            size = Globals.NUM_OF_FILES_PER_BLOCK * Globals.SIZE_OF_FILE
            
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
                           'ParentList' : [self.createDBSFile({'LogicalFileName':parentFileName})]
                          }
                self.files[blockName].append(self.createDBSFile(dbsFile))
        
        return self.blocks[dataset]
    
    def createDBSFile(self, dbsFile = {}):
        defaultDBSFile = {'Checksum': "123456",
                          'NumberOfEvents': Globals.NUM_OF_EVENTS_PER_FILE,
                          'FileSize': Globals.SIZE_OF_FILE,
                          'ParentList': []
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
                
    