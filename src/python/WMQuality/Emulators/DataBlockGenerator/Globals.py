from builtins import object
NOT_EXIST_DATASET = 'thisdoesntexist'
PILEUP_DATASET = '/HighPileUp/Run2011A-v1/RAW'

SITES = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']

global _BLOCK_LOCATIONS

_BLOCK_LOCATIONS = {}

def getSites(block):
    if block in _BLOCK_LOCATIONS:
        return _BLOCK_LOCATIONS[block]

    if block.split('#')[0] == PILEUP_DATASET:
        # Pileup is at a single site
        sites = ['T2_XX_SiteC']
        _BLOCK_LOCATIONS[block] = sites
    elif block.endswith('#1'):
        sites  = ['T2_XX_SiteA']
        _BLOCK_LOCATIONS[block] = sites
    elif block.endswith('#2'):
        sites = ['T2_XX_SiteA', 'T2_XX_SiteB']
        _BLOCK_LOCATIONS[block] = sites
    else:
        sites = ['T2_XX_SiteA', 'T2_XX_SiteB','T2_XX_SiteC']
    return sites

# This can be replace to PhEDEx subscription call
# emulator
def moveBlock(blockSites):
    _BLOCK_LOCATIONS.update(blockSites)

class GlobalParams(object):

    # WARNING: do not change the default value:
    # unittests will break.
    # If they are needed to be changed in some unittests
    # or other situation, use the setter.
    # If they are changed in the unittest, reset to default values in tearDown
    # (otherwise buildbot test will have unexpected result,
    # since it will run several unittests in one process)

    _num_of_blocks_per_dataset = 2
    _num_of_files_per_block = 5
    _num_of_runs_per_file = 1
    _num_of_lumis_per_block = 2
    _num_of_events_per_file = 1000
    _size_of_file = 20000000
    _blocks_open_for_writing = False

    @staticmethod
    def numOfBlocksPerDataset():
        return GlobalParams._num_of_blocks_per_dataset

    @staticmethod
    def numOfFilesPerBlock():
        return GlobalParams._num_of_files_per_block

    @staticmethod
    def numOfRunsPerFile():
        return GlobalParams._num_of_runs_per_file

    @staticmethod
    def numOfEventsPerFile():
        return GlobalParams._num_of_events_per_file

    @staticmethod
    def numOfLumisPerBlock():
        #It's really the number of lumis per file
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
    def setNumOfRunsPerFile(numOfRunsPerFile):
        GlobalParams._num_of_runs_per_file = numOfRunsPerFile

    @staticmethod
    def setNumOfEventsPerFile(numOfEventsPerFile):
        GlobalParams._num_of_events_per_file = numOfEventsPerFile

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

    @staticmethod
    def resetParams():
        """
        reset the parameter to default value
        """
        GlobalParams._num_of_blocks_per_dataset = 2
        GlobalParams._num_of_files_per_block = 5
        GlobalParams._num_of_runs_per_file = 1
        GlobalParams._num_of_lumis_per_block = 2
        GlobalParams._num_of_events_per_file = 1000
        GlobalParams._size_of_file = 20000000
        GlobalParams._blocks_open_for_writing = False
