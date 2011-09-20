#!/usr/bin/env python
"""
_ResourceControl_

Library from manipulating and querying the resource control database.
"""

from WMCore.DAOFactory import DAOFactory
from WMCore.WMConnectionBase import WMConnectionBase
from WMCore.WMException import WMException

class ResourceControlException(WMException):
    """
    _ResourceControlException_

    Exception for the ResourceControl mechanisms
    """
    pass


class ResourceControl(WMConnectionBase):
    def __init__(self):
        WMConnectionBase.__init__(self, daoPackage = "WMCore.ResourceControl")
        self.wmbsDAOFactory = DAOFactory(package = "WMCore.WMBS",
                                         logger = self.logger,
                                         dbinterface = self.dbi)
        return

    def insertSite(self, siteName, jobSlots = 0, seName = None,
                   ceName = None, cmsName = None, plugin = None):
        """
        _insertSite_

        Insert a site into WMBS.  The site must be inserted before any
        thresholds can be added.
        """
        insertAction = self.wmbsDAOFactory(classname = "Locations.New")
        insertAction.execute(siteName = siteName, jobSlots = jobSlots,
                             seName = seName, ceName = ceName,
                             plugin = plugin, cmsName = cmsName,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        return

    def listSiteInfo(self, siteName):
        """
        _listSiteInfo_

        List the site name, ce name, se name and number of job slots for a
        given site.
        """
        listAction = self.wmbsDAOFactory(classname = "Locations.GetSiteInfo")
        result = listAction.execute(siteName = siteName,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        if len(result) == 0:
            return None
        return result[0]

    def insertThreshold(self, siteName, taskType, maxSlots, priority = None):
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
                             maxSlots = maxSlots,
                             priority = priority,
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

    def listWorkloadsForTaskSite(self, taskType, siteName):
        """
        _listWorkflowsForTaskSite_

        For the given task and site list the number of jobs running for each
        task.
        """
        listActions = self.daofactory(classname = "ListWorkloadsForTaskSite")
        return listActions.execute(taskType, siteName,
                                   conn = self.getDBConn(),
                                   transaction = self.existingTransaction())

    def setJobSlotsForSite(self, siteName, jobSlots):
        """
        _setJobSlotsForSite_

        Set the number of job slots for the given site.
        """
        slotsAction = self.daofactory(classname = "SetJobSlotsForSite")
        slotsAction.execute(siteName, jobSlots, conn = self.getDBConn(),
                            transaction = self.existingTransaction())

    def thresholdBySite(self, siteName):
        """
        _thresholdBySite_
        
        List the thresholds of a single site
        """
        listActions = self.daofactory(classname = "ThresholdBySite")
        return listActions.execute(site = siteName,
                                   conn = self.getDBConn(),
                                   transaction = self.existingTransaction())


    def insertAllSEs(self, siteName, jobSlots = 0, ceName = None, plugin = None,
                     taskList = []):
        """
        _insertAllSEs_

        Insert all SEs into WMBS ResourceControl
        This uses the Services.SiteDB to insert all SEs under a common
        CE.  It is meant to be used with WMS submission.

        Sites will be named siteName_SEName

        It expects a taskList of the following form:

        [{'taskType': taskType, 'priority': priority, 'maxSlots': maxSlots}]

        for each entry in the taskList, a threshold is inserted into the database
        for EVERY SE
        """

        from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
        siteDB = SiteDBJSON()

        cmsNames = siteDB.getAllCMSNames()
        for cmsName in cmsNames:
            seNames = siteDB.cmsNametoSE(cmsName)
            for SE in seNames:
                sName = '%s_%s' % (siteName, SE)
                self.insertSite(siteName = sName, jobSlots = jobSlots, seName = SE,
                                ceName = ceName, cmsName = cmsName, plugin = plugin)
                for task in taskList:
                    if not task.has_key('maxSlots') or not task.has_key('taskType') \
                           or not task.has_key('priority'):
                        msg =  "Incomplete task in taskList for ResourceControl.insertAllSEs\n"
                        msg += task
                        raise ResourceControlException(msg)
                    self.insertThreshold(siteName = sName, taskType = task['taskType'],
                                         maxSlots = task['maxSlots'], priority = task['priority'])


        return

        
