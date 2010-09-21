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

    sql = """select wl.se_name AS se_name, wfl.fileid AS id from wmbs_location wl
                INNER JOIN wmbs_file_location wfl ON wfl.location = wl.id
                WHERE wfl.fileid = :id
                """
    
