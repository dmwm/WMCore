#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Workflow object representing how Release Validation workflows
have to be dealt with in the input data placement (MSTransferor)
"""

from WMCore.MicroService.MSTransferor.DataStructs.Workflow import Workflow
from WMCore.Services.Rucio.RucioUtils import GROUPING_ALL


class RelValWorkflow(Workflow):
    """
    Class to represent a RelVal workflow in the context
    of input data placement in MSTransferor
    """

    def getRucioGrouping(self):
        """
        Returns the rucio rule grouping for RelVal workflows,
        which must hold all the input data under the same RSE in
        order to harvest full stats.

        :return: a string with the required DID grouping
        """
        return GROUPING_ALL
