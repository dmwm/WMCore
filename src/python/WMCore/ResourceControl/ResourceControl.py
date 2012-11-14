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

    def insertSite(self, siteName, pendingSlots = 0, runningSlots = 0,
                   seName = None, ceName = None, cmsName = None,
                   plugin = None):
        """
        _insertSite_

        Insert a site into WMBS.  The site must be inserted before any
        thresholds can be added.
        """
        insertAction = self.wmbsDAOFactory(classname = "Locations.New")
        insertAction.execute(siteName = siteName, pendingSlots = pendingSlots,
                             runningSlots = runningSlots,
                             seName = seName, ceName = ceName,
                             plugin = plugin, cmsName = cmsName,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        return

    def changeSiteState(self, siteName, state):
        """
        _changeSiteState_
        Set a site to some of the possible states

        """
        setStateAction = self.wmbsDAOFactory(classname = "Locations.SetState")
        setStateAction.execute(siteName = siteName, state = state,
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

    def listSiteInfo(self, siteName):
        """
        _listSiteInfo_

        List the site name, ce name, se name and number of job slots for a
        given site.
        """
        listAction = self.wmbsDAOFactory(classname = "Locations.GetSiteInfo")
        results = listAction.execute(siteName = siteName,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        if len(results) == 0:
            return None

        # We get a row back for every single SE.  Return a single dict with a
        # list in the SE field.
        seNames = []
        for result in results:
            seNames.append(result["se_name"])

        results[0]["se_name"] = seNames
        return results[0]

    def insertThreshold(self, siteName, taskType, maxSlots, pendingSlots, priority = None):
        """
        _insertThreshold_

        Insert a threshold into the Resource Control database.  If the threshold
        already exists it will be updated.
        taskType may be a list of tasks. Update each task individually.
        """
        existingTransaction = self.beginTransaction()

        subTypeAction = self.wmbsDAOFactory(classname = "Subscriptions.InsertType")
        subTypeAction.execute(subType = taskType, conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        insertAction = self.daofactory(classname = "InsertThreshold")
        if type(taskType) == type([]):
            for singleTask in taskType:
                insertAction.execute(siteName = siteName,
                                     taskType = singleTask,
                                     maxSlots = maxSlots,
                                     pendingSlots = pendingSlots,
                                     priority = priority,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        else:
            insertAction.execute(siteName = siteName,
                                     taskType = taskType,
                                     maxSlots = maxSlots,
                                     pendingSlots = pendingSlots,
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
        returned in the form of a two level dictionary.  The first level is
        keyed by the site name.  The second level has the following keys:
          cms_name            - CMS name of the site
          se_names            - List with associated SEs
          state               - State of the site
          total_pending_slots - Total number of pending slots available at the site
          total_running_slots - Total number of running slots available at the site
          total_pending_jobs  - Total jobs pending at the site
          total_running_jobs  - Total jobs running at the site
          thresholds          - List of dictionaries with threshold information ordered by descending priority
        The threshold dictionaries have the following keys:
          task_type           - Type of the task associated with the thresholds
          max_slots           - Maximum running slots for the task type
          pending_slots       - Maximum pending slots for the task type
          task_running_jobs   - Running jobs for the task type
          task_pending_jobs   - Pending jobs for the task type
          priority            - Priority assigned to the task type
        """
        listAction = self.daofactory(classname = "ListThresholdsForSubmit")
        return listAction.execute(conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

    def listThresholdsForCreate(self):
        """
        _listThresholdsForCreate_

        This will return a two level dictionary with the first level being
        keyed by site name.  The second level will have the following keys:
          total_slots - Total number of pending slots available at the site
          pending_jobs - Total number of jobs pending at the site
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

    def setJobSlotsForSite(self, siteName, pendingJobSlots = None,
                           runningJobSlots = None):
        """
        _setJobSlotsForSite_

        Set the number of running and/or pending job slots for the given site.
        """
        if pendingJobSlots != None:
            pendingSlotsAction = self.daofactory(classname = "SetPendingJobSlotsForSite")
            pendingSlotsAction.execute(siteName, pendingJobSlots,
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

        if runningJobSlots != None:
            runningSlotsAction = self.daofactory(classname = "SetRunningJobSlotsForSite")
            runningSlotsAction.execute(siteName, runningJobSlots,
                                       conn = self.getDBConn(),
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


    def insertAllSEs(self, siteName, pendingSlots = 0, runningSlots = 0,
                     ceName = None, plugin = None,
                     taskList = []):
        """
        _insertAllSEs_

        Insert all SEs into WMBS ResourceControl
        This uses the Services.SiteDB to insert all SEs under a common
        CE.  It is meant to be used with WMS submission.

        Sites will be named siteName_SEName

        It expects a taskList of the following form:

        [{'taskType': taskType, 'priority': priority, 'maxSlots': maxSlots, 'pendingSlots' : pendingSlots}]

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
                self.insertSite(siteName = sName, pendingSlots = pendingSlots,
                                seName = SE, runningSlots = runningSlots,
                                ceName = ceName, cmsName = cmsName, plugin = plugin)
                for task in taskList:
                    if not task.has_key('maxSlots') or not task.has_key('taskType') \
                           or not task.has_key('priority'):
                        msg =  "Incomplete task in taskList for ResourceControl.insertAllSEs\n"
                        msg += task
                        raise ResourceControlException(msg)
                    self.insertThreshold(siteName = sName, taskType = task['taskType'],
                                         maxSlots = task['maxSlots'], pendingSlots = task['pendingSlots'], priority = task['priority'])


        return
