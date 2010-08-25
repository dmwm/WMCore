#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets in Oracle

"""
__revision__ = "$Id: FindAlgos.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindAlgos import FindAlgos as MySQLFindAlgos

class FindAlgos(MySQLFindAlgos):
    """
Oracle implementation to find algos in datasets
    """
    

    
