#!/usr/bin/env python
"""
Oracle implementation of Jobs.LoadOutputID

Load the ID of the output fileset for a job
"""
__all__ = []
__revision__ = "$Id: LoadOutputID.py,v 1.1 2010/02/26 21:24:14 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadOutputID import LoadOutputID as MySQLLoadOutputID

class LoadOutputID(MySQLLoadOutputID):
    """
    
    Load the ID of the output fileset for a job

    """
