"""
_GetCompletedBlocks_

Oracle implementation of RucioInjector.GetComletedBlocks

"""

from __future__ import division
from __future__ import print_function

from WMComponent.RucioInjector.Database.MySQL.GetCompletedBlocks import GetCompletedBlocks as MySQLGetCompletedBlocks


class GetCompletedBlocks(MySQLGetCompletedBlocks):
    pass
