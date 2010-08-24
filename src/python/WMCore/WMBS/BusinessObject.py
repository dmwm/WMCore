#!/usr/bin/env python
#pylint: disable-msg=R0903
"""
_BusinessObject_

A simple base object representing a generic WMBS BusinessObject

"""

__revision__ = "$Id: BusinessObject.py,v 1.2 2008/10/22 17:51:53 metson Exp $"
__version__ = "$Revision: 1.2 $"
from WMCore.DAOFactory import DAOFactory

class BusinessObject(object):
    """
    A simple class that holds a database connection, a logger and a dao factory
    """
    def __init__(self, logger=None, dbfactory=None):
        """
        Create the BusinessObject
        """
        self.dbfactory = dbfactory
        self.logger = logger
        self.daofactory = DAOFactory(package='WMCore.WMBS', 
                                     logger=self.logger, 
                                     dbinterface=self.dbfactory.connect())