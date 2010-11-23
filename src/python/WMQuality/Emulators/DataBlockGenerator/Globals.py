NOT_EXIST_DATASET = 'thisdoesntexist'

SITES = ['SiteA', 'SiteB', 'SiteC']

global _BLOCK_LOCATIONS

_BLOCK_LOCATIONS = {}

#def getSites(block):
#    if _BLOCK_LOCATIONS.has_key(block):
#        return _BLOCK_LOCATIONS[block]
#    return SITES

def getSites(block):
    if _BLOCK_LOCATIONS.has_key(block):
        return _BLOCK_LOCATIONS[block]

    if block.endswith('#1'):
        sites  = ['SiteA']
        _BLOCK_LOCATIONS[block] = sites
    elif block.endswith('#2'):
        sites = ['SiteA', 'SiteB_MSS']
        _BLOCK_LOCATIONS[block] = sites
    else:
         sites = ['SiteA', 'SiteB','SiteC']
    return sites

# This can be replace to PhEDEx subscription call
# emulator
def moveBlock(blockSites):
    _BLOCK_LOCATIONS.update(blockSites)

class GlobalParams(object):
    
    _num_of_blocks_per_dataset = 2
    _num_of_files_per_block = 5
    _num_of_lumis_per_block = 2
    _num_of_events_per_file = 1000
    _size_of_file = 20000000
    
    @staticmethod
    def numOfBlocksPerDataset():
        return GlobalParams._num_of_blocks_per_dataset
    
    @staticmethod
    def numOfFilesPerBlock():
        return GlobalParams._num_of_files_per_block
    
    @staticmethod
    def numOfEventsPerFile():
        return GlobalParams._num_of_events_per_file
    
    @staticmethod
    def numOfLumisPerBlock():
        return GlobalParams._num_of_lumis_per_block

    @staticmethod
    def sizeOfFile():
        return GlobalParams._size_of_file
    
    @staticmethod
    def setNumOfBlocksPerDataset(numOfBlocksPerDataset):
        GlobalParams._num_of_blocks_per_dataset = numOfBlocksPerDataset
    
    @staticmethod
    def setNumOfFilesPerBlock(numOfFilesPerBlock):
        GlobalParams._num_of_files_per_block = numOfFilesPerBlock
    
    @staticmethod
    def setNumOfEventsPerFile(numOfEventsPerFile):
        GlobalParams._num_of_events_per_filet = numOfEventsPerFile
    
    @staticmethod
    def setNumOfLumisPerBlock(numOfLumisPerBlock):
        GlobalParams._num_of_lumis_per_block = numOfLumisPerBlock

    @staticmethod
    def setSizeOfFile(sizeOfFile):
        GlobalParams._size_of_file = sizeOfFile

    @staticmethod
    def getRunNumberForBlock(blockName):
        #assumes blockName is contains number after '#'
        return int(blockName.split('#')[-1])
    
        