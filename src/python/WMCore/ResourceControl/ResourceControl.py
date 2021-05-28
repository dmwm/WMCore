#!/usr/bin/env python
"""
_ResourceControl_

Library from manipulating and querying the resource control database.
"""

from builtins import str, bytes

import time
from WMCore.BossAir.BossAirAPI import BossAirAPI
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
    def __init__(self, config=None):
        WMConnectionBase.__init__(self, daoPackage="WMCore.ResourceControl")
        self.wmbsDAOFactory = DAOFactory(package="WMCore.WMBS",
                                         logger=self.logger,
                                         dbinterface=self.dbi)
        self.config = config
        return

    def insertSite(self, siteName, pendingSlots=0, runningSlots=0,
                   pnn=None, ceName=None, cmsName=None,
                   plugin=None):
        """
        _insertSite_

        Insert a site into WMBS tables, in the following order:
         1. first add PSN/site info into wmbs_location
         2. then add PNNs into wmbs_pnns
         3. at last, create a mapping of PSN/PNN in wmbs_location_pnns
        """
        timeNow = int(time.time())
        insertAction = self.wmbsDAOFactory(classname="Locations.New")
        insertAction.execute(siteName=siteName, pendingSlots=pendingSlots,
                             runningSlots=runningSlots,
                             pnn=pnn, ceName=ceName,
                             plugin=plugin, cmsName=cmsName, stateTime=timeNow,
                             conn=self.getDBConn(),
                             transaction=self.existingTransaction())
        return

    def insertPNNs(self, pnns):
        """
        _insertPNNs_

        Insert a list of standalone PNNs into WMBS (usually used for MSS nodes)
        """
        addAction = self.wmbsDAOFactory(classname="Locations.AddPNNs")
        addAction.execute(pnns=pnns, conn=self.getDBConn(),
                          transaction=self.existingTransaction())
        return

    def changeSiteState(self, siteName, state):
        """
        _changeSiteState_
        Set a site to some of the possible states and perform
        proper actions with the jobs, according to the state
        """
        timeNow = int(time.time())
        state2ExitCode = {"Aborted": 71301,
                          "Draining": 71302,
                          "Down": 71303}
        executingJobs = self.wmbsDAOFactory(classname="Jobs.ListByState")
        jobInfo = executingJobs.execute(state='executing')

        if jobInfo:
            bossAir = BossAirAPI(self.config)
            jobtokill = bossAir.updateSiteInformation(jobInfo, siteName, state in state2ExitCode)

            ercode = state2ExitCode.get(state, 71300)
            bossAir.kill(jobtokill, errorCode=ercode)

        # only now that jobs were updated by the plugin, we flip the site state
        setStateAction = self.wmbsDAOFactory(classname="Locations.SetState")
        setStateAction.execute(siteName=siteName, state=state, stateTime=timeNow,
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        return

    def listCurrentSites(self):
        """
        _listCurrentSites_

        List all the sites currently in Resource Control
        """
        listAction = self.daofactory(classname="ListCurrentSites")
        sitesArray = listAction.execute(conn=self.getDBConn(),
                                        transaction=self.existingTransaction())
        return [entry['site'] for entry in sitesArray]

    def listSitesSlots(self):
        """
        _listSitesSlots_

        List all sites, their slots and state available in Resource Control
        """
        listAction = self.daofactory(classname="ListSitesSlotsState")
        results = listAction.execute(conn=self.getDBConn(),
                                     transaction=self.existingTransaction())
        return results

    def listSiteInfo(self, siteName):
        """
        _listSiteInfo_

        List the site name, SE name, CE name, pending and running slots,
        plugin, cms name and state for a given site.
        """
        listAction = self.wmbsDAOFactory(classname="Locations.GetSiteInfo")
        results = listAction.execute(siteName=siteName,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())
        if len(results) == 0:
            return None

        # We get a row back for every single SE.  Return a single dict with a
        # list in the SE field.
        pnns = []
        for result in results:
            pnns.append(result["pnn"])

        results[0]["pnn"] = pnns
        return results[0]

    def changeTaskPriority(self, taskType, priority):
        """
        _changeTaskPriority_

        Change the priority of a sub task in WMBS. If the task
        type doesn't exist already then it will be added first.
        """
        existingTransaction = self.beginTransaction()
        subTypeAction = self.wmbsDAOFactory(classname="Subscriptions.InsertType")
        subTypeAction.execute(subType=taskType,
                              priority=priority,
                              conn=self.getDBConn(),
                              transaction=existingTransaction)
        self.commitTransaction(existingTransaction)
        return

    def insertThreshold(self, siteName, taskType, maxSlots, pendingSlots):
        """
        _insertThreshold_

        Insert a threshold into the Resource Control database.  If the threshold
        already exists it will be updated.
        taskType may be a list of tasks. Update each task individually.
        """
        existingTransaction = self.beginTransaction()

        subTypeAction = self.wmbsDAOFactory(classname="Subscriptions.InsertType")
        insertAction = self.daofactory(classname="InsertThreshold")
        if isinstance(taskType, (str, bytes)):
            taskType = [taskType]
        for singleTask in taskType:
            subTypeAction.execute(subType=singleTask,
                                  conn=self.getDBConn(),
                                  transaction=self.existingTransaction())
            insertAction.execute(siteName=siteName,
                                 taskType=singleTask,
                                 maxSlots=maxSlots,
                                 pendingSlots=pendingSlots,
                                 conn=self.getDBConn(),
                                 transaction=self.existingTransaction())

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
          pnns                - List with associated PNNs
          state               - State of the site
          state_time          - Timestamp in which the site joined the current state
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
          wf_highest_priority - highest priority value among pending and running joba
        """
        listAction = self.daofactory(classname="ListThresholdsForSubmit")
        return listAction.execute(conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

    def listThresholdsForCreate(self):
        """
        _listThresholdsForCreate_

        This will return a three level dictionary with the first level being
        keyed by site name.  The second level will have the following keys:
          total_slots - Total number of pending slots available at the site
          pending_jobs - Total number of jobs pending at the site per priority level, it is a dictionary
        """
        listAction = self.daofactory(classname="ListThresholdsForCreate")
        return listAction.execute(conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

    def listWorkloadsForTaskSite(self, taskType, siteName):
        """
        _listWorkflowsForTaskSite_

        For the given task and site list the number of jobs running for each
        task.
        """
        listActions = self.daofactory(classname="ListWorkloadsForTaskSite")
        return listActions.execute(taskType, siteName,
                                   conn=self.getDBConn(),
                                   transaction=self.existingTransaction())

    def setJobSlotsForSite(self, siteName, pendingJobSlots=None,
                           runningJobSlots=None):
        """
        _setJobSlotsForSite_

        Set the number of running and/or pending job slots for the given site.
        """
        if pendingJobSlots is not None:
            pendingSlotsAction = self.daofactory(classname="SetPendingJobSlotsForSite")
            pendingSlotsAction.execute(siteName, pendingJobSlots,
                                       conn=self.getDBConn(),
                                       transaction=self.existingTransaction())

        if runningJobSlots is not None:
            runningSlotsAction = self.daofactory(classname="SetRunningJobSlotsForSite")
            runningSlotsAction.execute(siteName, runningJobSlots,
                                       conn=self.getDBConn(),
                                       transaction=self.existingTransaction())

    def thresholdBySite(self, siteName):
        """
        _thresholdBySite_

        List the thresholds of a single site
        """
        listActions = self.daofactory(classname="ThresholdBySite")
        return listActions.execute(site=siteName,
                                   conn=self.getDBConn(),
                                   transaction=self.existingTransaction())
