#!/usr/bin/env python
"""
_BusinessObject_

A simple base object representing a generic WMBS BusinessObject

"""

__revision__ = "$Id: BusinessObject.py,v 1.1 2008/06/20 12:34:21 metson Exp $"
__version__ = "$Revision: 1.1 $"
from WMCore.DAOFactory import DAOFactory

class BusinessObject(object):
    def __init__(self, logger=None, dbfactory=None):
        """
        Create the BusinessObject
        """
        self.dbfactory = dbfactory
        self.logger = logger
        self.daofactory = DAOFactory(package='WMCore.WMBS', 
                                     logger=self.logger, 
                                     dbinterface=self.dbfactory.connect())