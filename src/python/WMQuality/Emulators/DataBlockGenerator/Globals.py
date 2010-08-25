NUM_OF_BLOCKS_PER_DATASET = 5
NUM_OF_FILES_PER_BLOCK = 5
NUM_OF_EVENTS_PER_FILE = 1000
SIZE_OF_FILE = 20000000

SITES = ['SiteA', 'SiteB', 'SiteC']

global _BLOCK_LOCATIONS

_BLOCK_LOCATIONS = {}

def getSites(block):
    if _BLOCK_LOCATIONS.has_key(block):
        return _BLOCK_LOCATIONS[block]
    return SITES[len(block) % len(SITES)]

def moveBlock(block, sites):
    _BLOCK_LOCATIONS[block] = sites
