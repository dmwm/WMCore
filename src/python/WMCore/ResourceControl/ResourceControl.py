#!/usr/bin/env python
"""
_ResourceControl_

Library from manipulating and querying the resource control database.
"""

__revision__ = "$Id: ResourceControl.py,v 1.2 2010/02/09 17:57:27 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory
from WMCore.WMConnectionBase import WMConnectionBase

class ResourceControl(WMConnectionBase):
    """
    Wrapper Object
    """
    def __init__(self):
        WMConnectionBase.__init__(self, daoPackage = "WMCore.ResourceControl")
        self.wmbsDAOFactory = DAOFactory(package = "WMCore.WMBS",
                                         logger = self.logger,
                                         dbinterface = self.dbi)
        return

    def insertSite(self, siteName, jobSlots = 0, seName = None, ceName = None):
        """
        _insertSite_

        Insert a site into WMBS.  The site must be inserted before any
        thresholds can be added.
        """
        insertAction = self.wmbsDAOFactory(classname = "Locations.New")
        insertAction.execute(siteName = siteName, jobSlots = jobSlots,
                             seName = seName, ceName = ceName,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        return

    def insertThreshold(self, siteName, taskType, minSlots, maxSlots):
        """
        _insertThreshold_

        Insert a threshold into the Resource Control database.  If the threshold
        already exists it will be updated.
        """
        existingTransaction = self.beginTransaction()
        
        subTypeAction = self.wmbsDAOFactory(classname = "Subscriptions.InsertType")
        subTypeAction.execute(subType = taskType, conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        insertAction = self.daofactory(classname = "InsertThreshold")
        insertAction.execute(siteName = siteName, taskType = taskType,
                             minSlots = minSlots, maxSlots = maxSlots,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def listThresholdsForSubmit(self):
        """
        _listThresholdsForSubmit_

        Retrieve a list of job threshold information as well as information on
        the number of jobs running for all the known sites.  This information is
        returned in the form of a three level dictionary.  The first level is
        keyed by the site name while the second is keyed by the task type.  The
        final level has the following keys:
          total_slots - Total number of slots available at the site
          task_running_jobs - Number of jobs for this task running at the site
          total_running_jobs - Total jobs running at the site
          min_slots - Minimum number of job slots for this task at the site
          max_slots - Maximum number of job slots for this task at the site
        """
        listAction = self.daofactory(classname = "ListThresholdsForSubmit")
        return listAction.execute(conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

    def listThresholdsForCreate(self):
        """
        _listThresholdsForCreate_

        This will return a two level dictionary with the first level being
        keyed by site name.  The second level will have the following keys:
          total_slots - Total number of slots available at the site
          running_jobs - Total number of jobs running at the site
        """
        listAction = self.daofactory(classname = "ListThresholdsForCreate")
        return listAction.execute(conn = self.getDBConn(),
                                  transaction = self.existingTransaction())        
