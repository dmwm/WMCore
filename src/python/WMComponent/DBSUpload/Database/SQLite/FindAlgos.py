#!/usr/bin/env python
"""
_DBSUpload.FindAlgos_

Find algos in datasets in SQLite

"""
__revision__ = "$Id: FindAlgos.py,v 1.1 2009/07/20 17:51:44 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindAlgos import FindAlgos as MySQLFindAlgos

class FindAlgos(MySQLFindAlgos):
    """
    SQLite implementation to find algos in datasets
    """
    

    
