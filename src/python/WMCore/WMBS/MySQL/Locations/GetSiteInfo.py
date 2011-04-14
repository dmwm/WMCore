#!/usr/bin/env python
"""
_GetSiteInfo_

MySQL implementation of Locations.GetSiteInfo
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

import logging

class GetSiteInfo(DBFormatter):
    """
    Grab all the relevant information for a given site.
    Usually useful only in the submitter
    """
    
    sql = "SELECT site_name, se_name, ce_name, job_slots, plugin FROM wmbs_location WHERE site_name = :site"


    def execute(self, siteName = None, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, {'site': siteName}, conn = conn, transaction = transaction)
        return self.formatDict(results)
