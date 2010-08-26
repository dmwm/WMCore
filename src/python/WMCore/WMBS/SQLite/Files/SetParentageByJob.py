#!/usr/bin/env python
"""
SQLite implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""
__all__ = []
__revision__ = "$Id: SetParentageByJob.py,v 1.1 2010/02/26 20:46:06 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.SetParentageByJob import SetParentageByJob as MySQLSetParentageByJob

class SetParentageByJob(MySQLSetParentageByJob):
    """
    
    Make the parentage link between a file and all the inputs of a given job

    """
