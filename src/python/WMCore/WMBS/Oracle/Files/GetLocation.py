"""
Oracle implementation of GetLocationFile
"""
from WMCore.WMBS.MySQL.Files.GetLocation import GetLocation \
     as GetLocationFileMySQL

class GetLocation(GetLocationFileMySQL):
    """
    _GetLocation_

    Oracle specific: file is reserved word

    """
    pass
