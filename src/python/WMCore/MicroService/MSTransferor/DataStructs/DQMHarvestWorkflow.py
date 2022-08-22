#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Workflow object representing how DQMHarvest workflows have to be dealt
with in the input data placement (MSTransferor)
"""

from WMCore.MicroService.MSTransferor.DataStructs.Workflow import Workflow
from WMCore.Services.Rucio.RucioUtils import GROUPING_ALL


class DQMHarvestWorkflow(Workflow):
    """
    Class to represent a DQMHarvest workflow in the context
    of input data placement in MSTransferor
    """

    def getInputData(self):
        """
        Returns all the primary data that has to be locked and
        transferred with Rucio.

        :return: a list of unique block names and an integer
                 with their total size
        """
        blockList = list(self.getPrimaryBlocks())
        totalBlockSize = sum([blockInfo['blockSize'] for blockInfo in self.getPrimaryBlocks().values()])
        return blockList, totalBlockSize

    def getRucioGrouping(self):
        """
        Returns the rucio rule grouping for DQMHarvest workflows,
        which must hold all the input data under the same RSE in
        order to harvest full stats.

        :return: a string with the required DID grouping
        """
        return GROUPING_ALL
