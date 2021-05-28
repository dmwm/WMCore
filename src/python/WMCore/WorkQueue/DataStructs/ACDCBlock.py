"""
Set of rules and functions
Handling resubmission Block construction
"""

from builtins import object

ACDC_PREFIX = "acdc"

class ACDCBlock(object):

    @staticmethod
    def name(wmspecName, taskName, offset, numOfFile):
        taskName = taskName.replace('/', ':')
        return "/%s/%s/%s/%s/%s" % (ACDC_PREFIX, wmspecName, taskName, offset,
                                     numOfFile)

    @staticmethod
    def checkBlockName(blockName):
        if blockName.split('/')[1] == ACDC_PREFIX:
            return True
        else:
            return False

    @staticmethod
    def splitBlockName(blockName):
        """ return False if the blockName is not acdc Block
            return original elements of block name in order as a dict
            {'SpecName': blockSplit[1],
             'TaskName': blockSplit[2],
             'Offset': int(blockSplit[3]),
             'NumOfFiles': int(blockSplit[4])},
             if it is acdc Block
            raise ValueError if the block is has wrong format
        """
        blockSplit = blockName.split('/')
        if blockSplit[1] != ACDC_PREFIX:
            return False
        elif len(blockSplit) != 6:
            msg = """blockName should contain prefix, wmspec name, task name,
                     offset and number of files %s""" % blockName
            raise ValueError(msg)
        else:
            return {'SpecName': str(blockSplit[2]),
                    'TaskName': str(blockSplit[3]).replace(':', '/'),
                    'Offset': int(blockSplit[4]),
                    'NumOfFiles': int(blockSplit[5])}
