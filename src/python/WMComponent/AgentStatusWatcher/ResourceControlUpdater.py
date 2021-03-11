"""
Perform cleanup actions
"""
from __future__ import division

__all__ = []

import logging
import traceback

from Utils.Timers import timeFunction
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.ReqMgrAux.ReqMgrAux import isDrainMode
from WMCore.Services.MonIT.Grafana import Grafana
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class ResourceControlUpdater(BaseWorkerThread):
    """
    Update site status and thresholds from SSB
    """

    def __init__(self, config):
        """
        Initialize
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        self.ssb2AgentStatus = {'enabled': 'Normal',
                                'drain': 'Draining',
                                'disabled': 'Down',
                                'test': 'Draining',
                                'unknown': None}
        self.tasksCPU = ['Processing', 'Production']
        self.tasksIO = ['Merge', 'Cleanup', 'Harvesting', 'LogCollect', 'Skim']
        self.minCPUSlots = 50
        self.minIOSlots = 25

        # get dashboard url, set metric columns from config
        _token = config.AgentStatusWatcher.grafanaToken
        self.grafanaURL = config.AgentStatusWatcher.grafanaURL
        self.grafanaAPIName = config.AgentStatusWatcher.grafanaSSB
        self.grafana = Grafana(_token, configDict={"endpoint": self.grafanaURL})

        # set pending percentages from config
        self.pendingSlotsSitePercent = config.AgentStatusWatcher.pendingSlotsSitePercent
        self.pendingSlotsTaskPercent = config.AgentStatusWatcher.pendingSlotsTaskPercent
        self.runningExpressPercent = config.AgentStatusWatcher.runningExpressPercent
        self.runningRepackPercent = config.AgentStatusWatcher.runningRepackPercent

        # sites forced to down
        self.forceSiteDown = getattr(config.AgentStatusWatcher, 'forceSiteDown', [])

        # agent team (for dynamic threshold) and queueParams (drain mode)
        self.teamName = config.Agent.teamName
        self.agentsNumByTeam = getattr(config.AgentStatusWatcher, 'defaultAgentsNumByTeam', 5)

        # only SSB sites
        self.onlySSB = config.AgentStatusWatcher.onlySSB

        # tier mode
        self.tier0Mode = hasattr(config, "Tier0Feeder")
        self.t1SitesCores = config.AgentStatusWatcher.t1SitesCores

        # switch this component on/off
        self.enabled = getattr(config.AgentStatusWatcher, 'enabled', True)

        # set resource control
        self.resourceControl = ResourceControl(config=self.config)

        # wmstats connection
        self.centralCouchDBReader = WMStatsReader(self.config.General.centralWMStatsURL)

    @timeFunction
    def algorithm(self, parameters):
        """
        _algorithm_

        Update site state and thresholds, based on differences between resource
        control database and info available in SSB.
            1. Get info from Resource Control database
            2. Get info from SSB
            3. Get information about teams and number of agents from WMStats
            4. Change site state when needed (this triggers a condor clasAd fetch)
            5. Change site thresholds when needed (and task thresholds)
        Sites from SSB are validated with PhEDEx node names
        """
        if not self.enabled:
            logging.info("This component is not enabled in the configuration. Doing nothing.")
            return

        try:
            sitesRC = self.resourceControl.listSitesSlots()
            logging.debug("Info from resource control: %s", sitesRC)
            # first, update site status
            ssbSiteStatus = self.getSiteStatus()
            self.checkStatusChanges(sitesRC, ssbSiteStatus)

            # now fetch site slots thresholds
            sitesSSB = self.getInfoFromSSB()
            if not sitesSSB:
                logging.error("One or more of the SSB metrics is down. Please contact the Dashboard team.")
                return

            logging.debug("Info from SSB: %s", sitesSSB)

            # get number of agents working in the same team (not in DrainMode)
            self.getAgentsByTeam()

            # Check which site slots need to be updated in the database
            self.checkSlotsChanges(sitesRC, sitesSSB)
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s", traceback.format_exc())
        logging.info("Resource control cycle finished updating site state and thresholds.")

    def getAgentsByTeam(self):
        """
        _getAgentsByTeam_

        Get the WMStats view for agents and teams
        """
        if isDrainMode(self.config):
            # maximize pending thresholds to get this agent drained ASAP
            self.agentsNumByTeam = 1
            return

        agentsByTeam = {}
        try:
            agentsByTeam = self.centralCouchDBReader.agentsByTeam(filterDrain=True)
        except Exception:
            logging.error("WMStats is not available or is unresponsive.")

        if not agentsByTeam:
            logging.warning("agentInfo couch view is not available, use default value %s", self.agentsNumByTeam)
        else:
            self.agentsNumByTeam = agentsByTeam.get(self.teamName, self.agentsNumByTeam)
            logging.debug("Agents connected to the same team (not in DrainMode): %d", self.agentsNumByTeam)
        return

    def getInfoFromSSB(self):
        """
        _getInfoFromSSB_

        Get site status, CPU bound and IO bound from dashboard (SSB).

        Returns a dict of dicts where the first key is the site name.
        """
        ssbCpuSlots = self.grafana.getSSBData("scap15min", "core_cpu_intensive", apiName=self.grafanaAPIName)
        ssbIoSlots = self.grafana.getSSBData("scap15min", "core_io_intensive", apiName=self.grafanaAPIName)

        ssbSiteSlots = self.thresholdsByVOName(ssbCpuSlots, ssbIoSlots)

        return ssbSiteSlots

    def checkStatusChanges(self, infoRC, infoSSB):
        """
        _checkStatusChanges_

        Checks which sites need to have their site state updated in
        resource control, based on:
          1. settings defined for the component (config.py)
          2. site state changes between SSB and RC
        """
        # First sets list of forced sites to down (HLT @FNAL is an example)
        for site in self.forceSiteDown:
            if site in infoRC and infoRC[site]['state'] != 'Down':
                logging.info("Forcing site %s to Down", site)
                self.updateSiteState(site, 'Down')
            infoSSB.pop(site, None)

        # if onlySSB sites, force all the sites not in SSB to down
        if self.onlySSB:
            for site in set(infoRC).difference(set(infoSSB)):
                if infoRC[site]['state'] != 'Down':
                    logging.info('Only SSBsites, forcing site %s to Down', site)
                    self.updateSiteState(site, 'Down')

        # normally set all the others
        for site in set(infoRC).intersection(set(infoSSB)):
            if infoRC[site]['state'] != infoSSB[site]['state']:
                logging.info('Changing %s state from %s to %s', site, infoRC[site]['state'], infoSSB[site]['state'])
                self.updateSiteState(site, infoSSB[site]['state'])
        return

    def checkSlotsChanges(self, infoRC, infoSSB):
        """
        _checkSlotsChanges_

        Checks which sites need to have their running and/or pending
        slots updated in resource control database, based on:
          1. number of agents connected to the same team
          2. and slots provided by the Dashboard team (SSB)

        If site slots are updated, then updates the task level too.
        """
        logging.debug("Settings for site and task pending slots: %s%% and %s%%", self.pendingSlotsSitePercent,
                      self.pendingSlotsTaskPercent)

        for site in set(infoRC).intersection(set(infoSSB)):
            if self.tier0Mode and site.startswith('T1_'):
                # T1 cores utilization for Tier0
                infoSSB[site]['slotsCPU'] *= self.t1SitesCores // 100
                infoSSB[site]['slotsIO'] *= self.t1SitesCores // 100
            else:
                # round very small sites to the bare minimum
                infoSSB[site]['slotsCPU'] = max(infoSSB[site]['slotsCPU'], self.minCPUSlots)
                infoSSB[site]['slotsIO'] = max(infoSSB[site]['slotsIO'], self.minIOSlots)
            CPUBound = infoSSB[site]['slotsCPU']
            IOBound = infoSSB[site]['slotsIO']

            sitePending = max(int(CPUBound / self.agentsNumByTeam * self.pendingSlotsSitePercent / 100),
                              self.minCPUSlots)

            # update site slots, if needed
            if infoRC[site]['running_slots'] != CPUBound or infoRC[site]['pending_slots'] != sitePending:
                # Update site running and pending slots
                logging.info("Updating %s site thresholds for pend/runn: %d/%d", site, sitePending, CPUBound)
                self.resourceControl.setJobSlotsForSite(site, pendingJobSlots=sitePending,
                                                        runningJobSlots=CPUBound)

            # now handle the task level thresholds
            self.checkTaskSlotsChanges(site, CPUBound, IOBound)

    def thresholdsByVOName(self, infoCpu, infoIo):
        """
        _thresholdsByVOName_

        Creates a dictionary with CPU and IO slots keyed by the site name.
        If any of the thresholds is missing or has an invalid value, the whole
        site thresholds is skipped.
        """
        ssbSiteSlots = {}
        for site in infoCpu:
            if infoCpu[site]['core_cpu_intensive'] is None:
                logging.warn('Site %s has invalid CPU thresholds in SSB. Taking no action', site)
            else:
                ssbSiteSlots[site] = {'slotsCPU': int(infoCpu[site]['core_cpu_intensive'])}

        # then iterate over the IO slots
        for site in infoIo:
            if infoIo[site]['core_io_intensive'] is None:
                logging.warn('Site %s has invalid IO thresholds in SSB. Taking no action', site)
            else:
                ssbSiteSlots[site]['slotsIO'] = int(infoIo[site]['core_io_intensive'])

        # Before proceeding, remove sites without both metrics
        for site in list(ssbSiteSlots):
            if len(ssbSiteSlots[site]) != 2:
                logging.warn("Site: %s has incomplete SSB metrics, see %s", site, ssbSiteSlots[site])
                ssbSiteSlots.pop(site)

        return ssbSiteSlots

    def getSiteStatus(self):
        """
        _getSiteStatus_

        Fetch site state from SSB and map it to agent state
        """
        ssbState = self.grafana.getSSBData("sts15min", "prod_status", apiName=self.grafanaAPIName)

        for site in list(ssbState):
            ssbStatus = ssbState[site]['prod_status']
            wmcoreStatus = self.getState(str(ssbStatus))
            if not wmcoreStatus:
                logging.warning("Site %s has an unknown SSB status '%s'. Skipping it!", site, ssbStatus)
                ssbState.pop(site, None)
            else:
                ssbState[site] = {'state': wmcoreStatus}

        return ssbState

    def getState(self, stateSSB):
        """
        _getState_

        Translates SSB states into resource control state
        """
        if self.tier0Mode and stateSSB == "test":
            # a test site for T0 has a different meaning than for production
            return "Normal"
        return self.ssb2AgentStatus.get(stateSSB)

    def updateSiteState(self, siteName, state):
        """
        _updateSiteState_

        Update only the site state in the resource control database.
        """
        try:
            self.resourceControl.changeSiteState(siteName, state)
        except Exception as ex:
            logging.error("Failed to update %s state to %s:", siteName, state)
            logging.error(str(ex))
            logging.error("Traceback: \n%s", traceback.format_exc())
        return

    def checkTaskSlotsChanges(self, siteName, CPUBound, IOBound):
        """
        _checkTaskSlotsChanges_

        Update the CPU and IOBound slots for a given site.
        """
        siteTaskSlots = self.resourceControl.thresholdBySite(siteName)
        taskCPUPending = max(int(CPUBound / self.agentsNumByTeam * self.pendingSlotsTaskPercent / 100),
                             self.minCPUSlots)
        taskIOPending = max(int(IOBound / self.agentsNumByTeam * self.pendingSlotsTaskPercent / 100), self.minIOSlots)

        updateTasks = False
        if siteTaskSlots[0]['task_type'] in self.tasksCPU and siteTaskSlots[0]['task_pending_slots'] != taskCPUPending:
            updateTasks = True
        elif siteTaskSlots[0]['task_type'] in self.tasksIO and siteTaskSlots[0]['task_pending_slots'] != taskIOPending:
            updateTasks = True

        if updateTasks:
            logging.info("Updating %s CPU tasks thresholds for pend/runn: %d/%d", siteName,
                         taskCPUPending, CPUBound)
            self.resourceControl.insertThreshold(siteName, taskType=self.tasksCPU, maxSlots=CPUBound,
                                                 pendingSlots=taskCPUPending)
            logging.info("Updating %s IO tasks thresholds for pend/runn: %d/%d", siteName,
                         taskIOPending, IOBound)
            self.resourceControl.insertThreshold(siteName, taskType=self.tasksIO, maxSlots=IOBound,
                                                 pendingSlots=taskIOPending)

        if self.tier0Mode:
            # Set task thresholds for Tier0
            logging.debug("Updating %s Express and Repack task thresholds.", siteName)
            expressSlots = int(CPUBound * self.runningExpressPercent / 100)
            pendingExpress = int(expressSlots * self.pendingSlotsTaskPercent / 100)
            self.resourceControl.insertThreshold(siteName, 'Express', expressSlots, pendingExpress)

            repackSlots = int(CPUBound * self.runningRepackPercent / 100)
            pendingRepack = int(repackSlots * self.pendingSlotsTaskPercent / 100)
            self.resourceControl.insertThreshold(siteName, 'Repack', repackSlots, pendingRepack)
