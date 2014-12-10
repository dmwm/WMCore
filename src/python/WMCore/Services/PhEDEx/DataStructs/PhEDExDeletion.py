"""
_PhEDExDeletion_

Data structure which contains the information for the deletion of a dataset/block in PhEDEx.

Created on May 29, 2013

@author: dballest
"""

class PhEDExDeletion(object):
    """
    _PhEDExDeletion_

    Data structure which contains PHEDEx fields for
    PhEDEx deletion data service
    """

    def __init__(self, datasetPathList, nodeList,
                 level = 'dataset', subscriptions = 'n', blocks = None,
                 comments = 'Deleted automatically by the WMAgent'):
        """
        Initialize PhEDEx deletion with default values
        """
        if type(datasetPathList) == str:
            datasetPathList = [datasetPathList]
        if type(nodeList) == str:
            nodeList = [nodeList]

        self.datasetPaths = set(datasetPathList)
        self.nodes = set(nodeList)

        self.level = level.lower()
        self.subscriptions = subscriptions
        self.blocks = blocks
        self.comments = comments

    def getDatasetsAndBlocks(self):
        """
        _getDatasetsAndBlocks_

        Get the block structure
        with datasets and blocks
        """
        return self.blocks
