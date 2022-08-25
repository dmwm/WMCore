#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Workflow object representing how growing workflows have to be dealt
with in the input data placement (MSTransferor)
"""

from WMCore.MicroService.MSTransferor.DataStructs.Workflow import Workflow
from WMCore.Services.Rucio.RucioUtils import GROUPING_DSET


class GrowingWorkflow(Workflow):
    """
    Class to represent a Growing workflow in the context
    of input data placement in MSTransferor
    """

    def getInputData(self):
        """
        Returns the primary dataset name to be locked and
        transferred with Rucio, instead of a list of blocks.

        :return: a list with the input dataset name and an integer
                 with their total size
        """
        inputContainer = [self.getInputDataset()]
        totalBlockSize = sum([blockInfo['blockSize'] for blockInfo in self.getPrimaryBlocks().values()])

        # the whole container must be locked
        return inputContainer, totalBlockSize

    def getRucioGrouping(self):
        """
        Returns the rucio rule grouping for growing workflows.
        Input blocks can be scattered all over the provided
        RSE expression.

        :return: a string with the required DID grouping
        """
        return GROUPING_DSET
