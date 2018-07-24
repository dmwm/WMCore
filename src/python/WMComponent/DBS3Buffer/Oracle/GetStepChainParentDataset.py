#!/usr/bin/env python
"""
_GetStepChainParentDataset_

Oracle implementation of DBSBuffer.GetStepChainParentDataset
"""

from WMComponent.DBS3Buffer.MySQL.GetStepChainParentDataset import GetStepChainParentDataset as MySQLGetStepChainParentDataset

class GetStepChainParentDataset(MySQLGetStepChainParentDataset):
    """
    _GetStepChainParentDataset_

    If given dataset name, get blockname
    """