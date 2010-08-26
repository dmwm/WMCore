#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

SQLite version

"""

__revision__ = "$Id: FindDASToUpload.py,v 1.1 2010/06/14 16:38:52 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSUpload.Database.MySQL.FindDASToUpload import FindDASToUpload as MySQLFindDASToUpload


class FindDASToUpload(MySQLFindDASToUpload):
    """
    Find Uploadable DAS

    """
