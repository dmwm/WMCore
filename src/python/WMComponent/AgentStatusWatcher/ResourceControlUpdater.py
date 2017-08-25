"""
Perform cleanup actions
"""
__all__ = []

import urllib2
import threading
import logging
import traceback
import json
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader


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

        # get dashboard url, set metric columns from config
        self.dashboard = config.AgentStatusWatcher.dashboard
        self.siteStatusMetric = config.AgentStatusWatcher.siteStatusMetric
        self.cpuBoundMetric = config.AgentStatusWatcher.cpuBoundMetric
        self.ioBoundMetric = config.AgentStatusWatcher.ioBoundMetric

        # set pending percentages from config
        self.pendingSlotsSitePercent = config.AgentStatusWatcher.pendingSlotsSitePercent
        self.pendingSlotsTaskPercent = config.AgentStatusWatcher.pendingSlotsTaskPercent
        self.runningExpressPercent = config.AgentStatusWatcher.runningExpressPercent
        self.runningRepackPercent = config.AgentStatusWatcher.runningRepackPercent

        # sites forced to down
        self.forceSiteDown = getattr(config.AgentStatusWatcher, 'forceSiteDown', [])

        # agent teams (for dynamic threshold) and queueParams (drain mode)
        self.teamNames = config.Agent.teamName
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
        self.centralCouchDBReader = WMStatsReader(self.config.AgentStatusWatcher.centralWMStatsURL)

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
            sitesSSB = self.getInfoFromSSB()
            if not sitesSSB:
                return
            logging.debug("Info from SSB: %s", sitesSSB)

            # Check which site states need to be updated in the database
            sitesRC = self.checkStatusChanges(sitesRC, sitesSSB)

            # get number of agents working in the same team (not in DrainMode)
            self.getAgentsByTeam()

            # Check which site slots need to be updated in the database
            self.checkSlotsChanges(sitesRC, sitesSSB, self.agentsNumByTeam)
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s", traceback.format_exc())
        logging.info("Resource control cycle finished updating site state and thresholds.")

    def getAgentsByTeam(self):
        """
        _getAgentsByTeam_

        Get the WMStats view about agents and teams
        """
        agentsByTeam = {}
        try:
            agentsByTeam = self.centralCouchDBReader.agentsByTeam(filterDrain=True)
        except Exception as ex:
            logging.error("WMStats is not available or is unresponsive.")

        if not agentsByTeam:
            logging.warning("agentInfo couch view is not available, use default value %s", self.agentsNumByTeam)
        else:
            agentsCount = []
            for team in self.teamNames.split(','):
                if team not in agentsByTeam:
                    agentsCount.append(self.agentsNumByTeam)
                else:
                    agentsCount.append(agentsByTeam[team])
            # If agent is in several teams, we choose the team with less agents
            self.agentsNumByTeam = min(min(agentsCount), self.agentsNumByTeam)
            logging.debug("Agents connected to the same team (not in DrainMode): %d", self.agentsNumByTeam)
        return

    def getInfoFromSSB(self):
        """
        _getInfoFromSSB_

        Get site status, CPU bound and IO bound from dashboard (SSB).

        Returns a dict of dicts where the first key is the site name.
        """
        # urls from site status board
        urlSiteState = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(
            self.siteStatusMetric)
        urlCpuBound = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(
            self.cpuBoundMetric)
        urlIoBound = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(
            self.ioBoundMetric)

        # get info from dashboard
        sites = urllib2.urlopen(urlSiteState).read()
        cpuBound = urllib2.urlopen(urlCpuBound).read()
        ioBound = urllib2.urlopen(urlIoBound).read()

        # parse from json format to dictionary, get only 'csvdata'
        ssbSiteState = json.loads(sites)['csvdata']
        ssbCpuSlots = json.loads(cpuBound)['csvdata']
        ssbIoSlots = json.loads(ioBound)['csvdata']

        # dict updated by these methods with status/thresholds info keyed by the site name
        ssbSiteSlots = {}
        self.siteStatusByVOName(ssbSiteState, ssbSiteSlots)
        self.thresholdsByVOName(ssbCpuSlots, ssbSiteSlots, slotsType='slotsCPU')
        self.thresholdsByVOName(ssbIoSlots, ssbSiteSlots, slotsType='slotsIO')

        # Now remove sites with state only, such that no updates are applied to them
        ssbSiteSlots = {k: v for k, v in ssbSiteSlots.iteritems() if len(v) == 3}

        if not ssbSiteSlots:
            logging.error("One or more of the SSB metrics is down. Please contact the Dashboard team.")
            return ssbSiteSlots

        return ssbSiteSlots

    def checkStatusChanges(self, infoRC, infoSSB):
        """
        _checkStatusChanges_

        Checks which sites need to have their site state updated in
        resource control, based on:
          1. settings defined for the component (config.py)
          2. site state changes between SSB and RC

        Returns the new infoRC dict (where a few key/value pairs were
        deleted - no need to update slots information)
        """
        # First sets list of forced sites to down (HLT @FNAL is an example)
        for site in self.forceSiteDown:
            if site in infoRC and infoRC[site]['state'] != 'Down':
                logging.info("Forcing site %s to Down", site)
                self.updateSiteState(site, 'Down')
            infoRC.pop(site, None)

        # if onlySSB sites, force all the sites not in SSB to down
        if self.onlySSB:
            for site in set(infoRC).difference(set(infoSSB)):
                if infoRC[site]['state'] != 'Down':
                    logging.info('Only SSBsites, forcing site %s to Down', site)
                    self.updateSiteState(site, 'Down')
                infoRC.pop(site, None)

        # this time don't update infoRC since we still want to update slots info
        for site in set(infoRC).intersection(set(infoSSB)):
            if infoRC[site]['state'] != infoSSB[site]['state']:
                logging.info('Changing %s state from %s to %s', site, infoRC[site]['state'], infoSSB[site]['state'])
                self.updateSiteState(site, infoSSB[site]['state'])
        return infoRC

    def checkSlotsChanges(self, infoRC, infoSSB, agentsCount):
        """
        _checkSlotsChanges_

        Checks which sites need to have their running and/or pending
        slots updated in resource control database, based on:
          1. number of agents connected to the same team
          2. and slots provided by the Dashboard team (SSB)

        If site slots are updated, then also updates its tasks.
        """
        tasksCPU = ['Processing', 'Production']
        tasksIO = ['Merge', 'Cleanup', 'Harvesting', 'LogCollect', 'Skim']
        minCPUSlots, minIOSlots = 50, 25

        logging.debug("Settings for site and task pending slots: %s%% and %s%%", self.pendingSlotsSitePercent,
                                                                                 self.pendingSlotsTaskPercent)

        for site in set(infoRC).intersection(set(infoSSB)):
            if self.tier0Mode and 'T1_' in site:
                # T1 cores utilization for Tier0
                infoSSB[site]['slotsCPU'] = infoSSB[site]['slotsCPU'] * self.t1SitesCores / 100
                infoSSB[site]['slotsIO'] = infoSSB[site]['slotsIO'] * self.t1SitesCores / 100

            # round very small sites to the bare minimum
            if infoSSB[site]['slotsCPU'] < minCPUSlots:
                infoSSB[site]['slotsCPU'] = minCPUSlots
            if infoSSB[site]['slotsIO'] < minIOSlots:
                infoSSB[site]['slotsIO'] = minIOSlots

            CPUBound = infoSSB[site]['slotsCPU']
            IOBound = infoSSB[site]['slotsIO']
            sitePending = max(int(CPUBound / agentsCount * self.pendingSlotsSitePercent / 100), minCPUSlots)
            taskCPUPending = max(int(CPUBound / agentsCount * self.pendingSlotsTaskPercent / 100), minCPUSlots)
            taskIOPending = max(int(IOBound / agentsCount * self.pendingSlotsTaskPercent / 100), minIOSlots)

            if infoRC[site]['running_slots'] != CPUBound or infoRC[site]['pending_slots'] != sitePending:
                # Update site running and pending slots
                logging.info("Updating %s site thresholds for pend/runn: %d/%d", site, sitePending, CPUBound)
                self.resourceControl.setJobSlotsForSite(site, pendingJobSlots=sitePending,
                                                        runningJobSlots=CPUBound)
                # Update site CPU tasks running and pending slots (large running slots)
                logging.debug("Updating %s tasksCPU thresholds for pend/runn: %d/%d", site, taskCPUPending,
                              CPUBound)
                for task in tasksCPU:
                    self.resourceControl.insertThreshold(site, taskType=task, maxSlots=CPUBound,
                                                         pendingSlots=taskCPUPending)
                # Update site IO tasks running and pending slots
                logging.debug("Updating %s tasksIO thresholds for pend/runn: %d/%d", site, taskIOPending,
                              IOBound)
                for task in tasksIO:
                    self.resourceControl.insertThreshold(site, taskType=task, maxSlots=IOBound,
                                                         pendingSlots=taskIOPending)

            if self.tier0Mode:
                # Set task thresholds for Tier0
                logging.debug("Updating %s Express and Repack task thresholds.", site)
                expressSlots = int(CPUBound * self.runningExpressPercent / 100)
                pendingExpress = int(expressSlots * self.pendingSlotsTaskPercent / 100)
                self.resourceControl.insertThreshold(site, 'Express', expressSlots, pendingExpress)

                repackSlots = int(CPUBound * self.runningRepackPercent / 100)
                pendingRepack = int(repackSlots * self.pendingSlotsTaskPercent / 100)
                self.resourceControl.insertThreshold(site, 'Repack', repackSlots, pendingRepack)

    def thresholdsByVOName(self, sites, ssbSiteSlots, slotsType):
        """
        _thresholdsByVOName_

        Updates the dict with CPU and IO slots, only for sites with a valid state
        """
        for site in sites:
            voname = site['VOName']
            value = site['Value']
            if voname in ssbSiteSlots:
                if value is None:
                    logging.warn('Site %s does not have thresholds in SSB. Taking no action', voname)
                    # then we better remove this site from our final dict
                    ssbSiteSlots.pop(voname)
                else:
                    ssbSiteSlots[voname][slotsType] = int(value)
            else:
                logging.warn('Found %s thresholds for site %s which has no state in SSB', slotsType, voname)
        return

    def siteStatusByVOName(self, sites, ssbSiteSlots):
        """
        _siteStatusByVOName_

        Creates an inner dictionary for each site that will contain
        the site state and the number of slots
        """
        for site in sites:
            voname = site['VOName']
            status = site['Status']
            if voname not in ssbSiteSlots:
                statusAgent = self.getState(str(status))
                if not statusAgent:
                    logging.error("Unkwown status '%s' for site %s, please check SSB", status, voname)
                else:
                    ssbSiteSlots[voname] = {'state': statusAgent}
            else:
                logging.error('I have a duplicated status entry in SSB for %s', voname)
        return

    def getState(self, stateSSB):
        """
        _getState_

        Translates SSB states into resource control state
        """
        ssb2agent = {'enabled': 'Normal',
                     'drain': 'Draining',
                     'disabled': 'Down',
                     'test': 'Draining'}
        # 'test' state behaviour varies between production and tier0 agents
        ssb2agent['test'] = 'Normal' if self.tier0Mode else "Draining"

        return ssb2agent.get(stateSSB)

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
