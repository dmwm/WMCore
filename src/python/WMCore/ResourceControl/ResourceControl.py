#!/usr/bin/env python


__revision__ = "$Id: ResourceControl.py,v 1.1 2009/10/05 19:59:20 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import os
import threading

from WMCore.WMFactory  import WMFactory
from WMCore.DAOFactory import DAOFactory


class ResourceControl:
    """
    Wrapper Object
    """

    def __init__(self):
        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.ResourceControl", logger = myThread.logger, dbinterface = myThread.dbi)


    def getThresholds(self, siteNames):
        """
        Wrapper for the getThreshold SQL calls
        """
        myThread  = threading.currentThread()
        #siteNames = list(siteNames)


        myThread.transaction.begin()
        
        action = self.daofactory(classname="GetThresholds")
        result = action.execute(siteName = siteNames, conn = None, transaction = myThread.transaction)

        myThread.transaction.commit()


        return result

    def insertThreshold(self, thresholdName = None, thresholdValue = None, siteNames = None, bulkList = None):
        """
        Wrapper for the insertThreshold SQL calls
        """
        myThread  = threading.currentThread()
        #siteNames = list(siteNames)


        myThread.transaction.begin()
        
        action = self.daofactory(classname="InsertThreshold")
        result = action.execute(siteName = siteNames, thresholdValue = thresholdValue, thresholdName = thresholdName, \
                                bulkList = bulkList, conn = None, transaction = myThread.transaction)

        myThread.transaction.commit()


        return result


    def insertSite(self, siteName, seName, ceName = None):
        """
        Wrapper for creating new sites
        """

        myThread = threading.currentThread()
        
        myThread.transaction.begin()
        
        action = self.daofactory(classname="InsertSite")
        result = action.execute(siteName = siteName, seName = seName, ceName = ceName, \
                                conn = None, transaction = myThread.transaction)

        myThread.transaction.commit()

        return
    
