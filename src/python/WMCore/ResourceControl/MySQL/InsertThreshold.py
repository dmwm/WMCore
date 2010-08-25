#!/usr/bin/env python

"""
_InsertThreshold_

This module inserts thresholds for the given sites for MySQL

"""

__revision__ = "$Id: InsertThreshold.py,v 1.1 2009/10/05 20:05:51 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class InsertThreshold(DBFormatter):
    """
    _InsertThreshold_
    
    This module finds thresholds for the given sites for MySQL
    """

    sql = """INSERT INTO rc_site_threshold (threshold_name, threshold_value, site_index) VALUES
               (:threshold_name, :threshold_value, (SELECT site_index FROM rc_site WHERE site_name = :site_name))"""

    def execute(self, thresholdName = None, thresholdValue = None, siteName = None, bulkList = None, conn = None, transaction = False):
        """
        Executes SQL command
        """

        if bulkList:
            #Bulk list should be a list of dictionaries of the form {'threshold_name': tn, 'threshold_value': tv, 'site_name': sn}
            binds = bulkList
        else:
            binds = {'threshold_name': thresholdName, 'threshold_value': thresholdValue, 'site_name': siteName}

        results = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return self.formatDict(results)
