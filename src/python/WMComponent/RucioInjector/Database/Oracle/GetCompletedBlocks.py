"""
_GetCompletedBlocks_

Oracle implementation of RucioInjector.GetComletedBlocks

"""


from WMComponent.RucioInjector.Database.MySQL.GetCompletedBlocks import GetCompletedBlocks as MySQLGetCompletedBlocks


class GetCompletedBlocks(MySQLGetCompletedBlocks):
    pass
