class DataBlockGenerator(object):
    
    def __init__(self):
        self.sites = ['SiteA', 'SiteB', 'SiteC']
        self.blocks = {}        
        self.locations = {}
        self.files = {}
     
    def _dataGenerator(self, dataset):
        
        #some simple process to generate blockw with consistency
        
        self.blocks[dataset] = []
        numOfBlocks =  len(dataset) % 3 + 3
        for i in numOfBlocks:
            blockName = "%s#%s" % (dataset, i)
            numOfFiles = i
            numOfEvents = i * 100
            size = i * 100000
            
            self.blocks[dataset].append(
                                    {'Name' : blockName,
                                     'NumEvents' : numOfEvents,
                                     'NumFiles' : numOfFiles,
                                     'Size' : size,
                                     'Parents' : ()}
                                     )
            self.locations[blockName] = self.sites[i%len(self.sites)]
            for fileID in numOfFiles:
                self.files[blockName].append(
                                {'Checksum': "123456",
                                 'LogicalFileName': "/store/data/%s/file%s" % (blockName, fileID),
                                 'NumberOfEvents': numOfEvents / numOfFiles,
                                 'FileSize': size / numOfFiles,
                                 'ParentList': []
                                 })
        return self.blocks[dataset]
    
    def getBlocks(self, dataset):
        
        if not self.blocks.has_key[dataset]:
            self._dataGenerator(dataset)
        return  self.blocks[dataset]
        
    def getFiles(self, block):
        if not self.files.has_key[block]:
            dataset = self.getDataset(block)
            self._dataGenerator(dataset)
        return  self.files[block]
    
    def getLocation(self, block):
        
        if not self.locations.has_key[block]:
            dataset = self.getDataset(block)
            self._dataGenerator(dataset)
        return  self.locations[block]
    
    def getDataset(self, block):
        return block.split('#')[0]
                
    