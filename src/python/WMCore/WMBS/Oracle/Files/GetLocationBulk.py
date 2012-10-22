#!/usr/bin/env python

"""
_GetLocationBulk_

Oracle implementation of Files.GetLocationBulk
"""


from WMCore.WMBS.MySQL.Files.GetLocationBulk import GetLocationBulk \
     as GetLocationBulkMySQL

class GetLocationBulk(GetLocationBulkMySQL):
    """
    _GetLocationBulk_

    Oracle specific: file is reserved word

    """

    pass
