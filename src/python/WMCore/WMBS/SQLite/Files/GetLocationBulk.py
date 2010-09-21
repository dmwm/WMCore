#!/usr/bin/env python

"""
_GetLocationBulk_

SQLite implementation of Files.GetLocationBulk
"""


from WMCore.WMBS.MySQL.Files.GetLocationBulk import GetLocationBulk \
     as GetLocationBulkMySQL

class GetLocationBulk(GetLocationBulkMySQL):
    """
    _GetLocationBulk_
    
    Identical to MySQL

    """
