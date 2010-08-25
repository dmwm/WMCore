#!/usr/bin/env python

"""
_InsertSite_

This module inserts sites for MySQL

"""

__revision__ = "$Id: InsertSite.py,v 1.1 2009/10/05 20:04:54 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class InsertSite(DBFormatter):
    """
    _InsertSite_
    
    This module inserts sites for MySQL
    
    """

    sql = """INSERT INTO rc_site (site_name, se_name, ce_name) VALUES (:site_name, :se_name, :ce_name)
    """


    def execute(self, siteName, seName, ceName = None, conn = None, transaction = False):
        """
        Insert new site into the Resource Control tables

        """
        
        binds = {'site_name': siteName, 'ce_name': ceName, 'se_name': seName}
        results = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return
