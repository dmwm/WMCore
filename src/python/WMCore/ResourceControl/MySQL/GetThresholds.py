#!/usr/bin/env python

"""
_GetThresholds_

This module finds thresholds for the given sites for MySQL

"""

__revision__ = "$Id: GetThresholds.py,v 1.1 2009/10/05 20:06:46 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class GetThresholds(DBFormatter):
    """
    _GetThresholds_
    
    This module finds thresholds for the given sites for MySQL
    """

    sql = """
    SELECT rct.threshold_name AS threshold_name, rct.threshold_value AS threshold_value, rcs.site_name AS site_name FROM
    rc_site rcs INNER JOIN
    rc_site_threshold rct
    ON rct.site_index = rcs.site_index
    WHERE rcs.site_name = :site_name
    """

    def execute(self, siteName, conn = None, transaction = False):
        """
        Executes SQL command
        """
        binds = {}
        if type(siteName) == list:
            binds = []
            for name in siteName:
                binds.append({'site_name': name})
        else:
            binds = {'site_name': siteName}

        results = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return self.formatDict(results)

    
