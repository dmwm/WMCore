"""
Perform cleanup actions
"""
__all__ = []

import urllib,urllib2, re, os
import threading
import logging
import traceback
import json
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Configuration import loadConfigurationFile

class ResourceControlUpdater(BaseWorkerThread):
    """
    Update site status and thresholds from SSB
    """
    def __init__(self, config):
        """
        Initialize 
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config
        self.setVariables(self.config)
        
    def setVariables(self, config):
        """
        load all the variables from the config file
        """
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
        
        # forced site list
        self.forcedSiteList = config.AgentStatusWatcher.forcedSiteList
        
        # agent teams (for dynamic threshold) and queueParams (drain mode)
        self.teamNames = config.Agent.teamName
        self.queueParams = config.WorkQueueManager.queueParams
        self.agentsNumByTeam = getattr(config.AgentStatusWatcher, 'defaultAgentsNumByTeam', 5)
                
        # only SSB sites
        self.onlySSB = config.AgentStatusWatcher.onlySSB
        
        # tier mode
        self.tier0Mode = hasattr(config, "Tier0Feeder")
        self.t1SitesCores = config.AgentStatusWatcher.t1SitesCores

        # switch this component on/off
        self.enabled = getattr(config.AgentStatusWatcher, 'enabled', True)
       
        
    def setup(self, parameters):
        """
        Set db connection and prepare resource control
        """
        # Interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set resource control
        self.resourceControl = ResourceControl(config = self.config)
        
        # wmstats connection 
        self.centralCouchDBReader = WMStatsReader(self.config.AgentStatusWatcher.centralWMStatsURL)
        
        # init variables
        self.agentsByTeam = {}

    def algorithm(self, parameters):
        """
        _algorithm_
        
        Update site info about state and thresholds
            1. Get information from SSB
            2. Get information about teams and agents from WMStats
            3. Set site status and set therholds for each valid site
        Sites from SSB are validated with PhEDEx node names
        """
        # set variables every polling cycle
        self.setVariables(self.config)
        if not self.enabled:
            logging.info("This component is not enabled in the configuration. Doing nothing.")
            return

        try:
            # Get sites in Resource Control
            currentSites = self.resourceControl.listCurrentSites()
            
            logging.debug("Starting algorithm, getting site info from SSB")
            stateBySite, slotsCPU, slotsIO = self.getInfoFromSSB()
            
            if not stateBySite or not slotsCPU or not slotsIO:
                logging.error("One or more of the SSB metrics is down. Please contact the Dashboard team.")
                return
            
            logging.debug("Setting status and thresholds for all sites, site pending: %s%%, task pending: %s%%" % 
                          (str(self.pendingSlotsSitePercent), str(self.pendingSlotsTaskPercent))) 
            
        
            # get number of agents working in the same team (not in DrainMode)
            agentsByTeam = self.getAgentsByTeam()
            if not agentsByTeam:
                logging.debug("agentInfo couch view is not available, use previous agent count %s" % self.agentsNumByTeam)
            else:
                self.agentsByTeam = agentsByTeam
                teams = self.teamNames.split(',')
                agentsCount = []
                for team in teams:
                    if self.agentsByTeam[team] == 0:
                        agentsCount.append(1)
                    else:
                        agentsCount.append(self.agentsByTeam[team])
                self.agentsNumByTeam = min(agentsCount) # If agent is in several teams, we choose the team with less agents
                logging.debug("Number of agents not in DrainMode running in the same team: %s" % str(self.agentsNumByTeam))
        
            # set site status and thresholds
            listSites = stateBySite.keys()
            if self.forcedSiteList:
                if set(self.forcedSiteList).issubset(set(listSites)):
                    listSites = self.forcedSiteList
                    logging.info("Forcing site list: %s" % (', '.join(self.forcedSiteList)))
                else:
                    listSites = self.forcedSiteList
                    logging.warn("Forcing site list: %s. Some site(s) are not in SSB" % (', '.join(self.forcedSiteList)))
                    
            for site in listSites:
                if site in currentSites:
                    sitestate = stateBySite.get(site,'Normal')
                    if site not in slotsCPU or site not in slotsIO:
                        pluginResponse = self.updateSiteInfo(site, sitestate, 0, 0, self.agentsNumByTeam)
                        if not pluginResponse: 
                            continue
                        logging.warn('Setting site %s to %s, forcing CPUBound: 0, IOBound: 0 due to missing information in SSB' % 
                                 (site, sitestate))
                        continue
                    
                    pluginResponse = self.updateSiteInfo(site, sitestate, slotsCPU[site], slotsIO[site], self.agentsNumByTeam)
                    if not pluginResponse:
                        continue
                    logging.info('Setting site %s to %s, CPUBound: %s, IOBound: %s' % 
                                 (site, sitestate, slotsCPU[site], slotsIO[site]))
                else:
                    logging.debug("Site '%s' has not been added to Resource Control" % site)
            
            # if onlySSB sites or forcedSiteList, force to down all the sites not in SSB/forcedSiteList
            if self.onlySSB or self.forcedSiteList:
                for site in set(currentSites).difference(set(listSites)):
                    pluginResponse = self.updateSiteInfo(site, 'Down', 0, 0, self.agentsNumByTeam)
                    if not pluginResponse:
                        continue
                    logging.info('Only SSBsites/forcedSiteList, forcing site %s to Down, CPUBound: 0, IOBound: 0' % site)
            
            logging.info("Resource update is completed, waiting for the next cycle.\n")
            
        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())

    def getAgentsByTeam(self):
        """
        _getAgentsByTeam_
        
        Get the WMStats view about agents and teams
        """
        agentsByTeam = []
        try:
            agentsByTeam = self.centralCouchDBReader.agentsByTeam()
            return agentsByTeam
        except Exception, ex:
            logging.error("WMStats is not available or is unresponsive. Don't divide thresholds by team")
            return agentsByTeam

    def getInfoFromSSB(self):
        """
        _getInfoFromSSB_
        
        Get site status, CPU bound and IO bound from dashboard (SSB)
        """
        # urls from site status board
        url_site_state = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(self.siteStatusMetric)
        url_cpu_bound = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(self.cpuBoundMetric)
        url_io_bound = self.dashboard + '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % str(self.ioBoundMetric)
        
        # get info from dashboard
        sites = urllib2.urlopen(url_site_state).read()
        cpu_bound = urllib2.urlopen(url_cpu_bound).read()
        io_bound = urllib2.urlopen(url_io_bound).read()
        
        # parse from json format to dictionary, get only 'csvdata'
        site_state = json.loads(sites)['csvdata']
        cpu_slots = json.loads(cpu_bound)['csvdata']
        io_slots = json.loads(io_bound)['csvdata']
        
        # dictionaries with status/thresholds info by VOName
        stateBySite = self.siteStatusByVOName(site_state)
        slotsCPU = self.thresholdsByVOName(cpu_slots)
        slotsIO = self.thresholdsByVOName(io_slots)
        
        return stateBySite, slotsCPU, slotsIO
        
    def thresholdsByVOName(self, sites):
        """
        _thresholdsByVOName_
        
        Creates a dictionary with keys->VOName and values->threshold: 
        """
        thresholdbyVOName = {}
        for site in sites:
            voname = site['VOName']
            value = site['Value']
            if voname not in thresholdbyVOName:
                if value is None: 
                    logging.warn('Site %s does not have threholds in SSB, assuming 0' % voname) 
                    thresholdbyVOName[voname] = 0
                else:
                    thresholdbyVOName[voname] = int(value)
            else:
                logging.error('I have a duplicated threshold entry in SSB for %s' % voname) 
        return thresholdbyVOName
    
    def siteStatusByVOName(self, sites):
        """
        _siteStatusByVOName_
        
        Creates a dictionary with keys->VOName and values->status:
        """
        statusBySite = {}
        for site in sites:
            voname = site['VOName']
            status = site['Status']
            if voname not in statusBySite:
                if not status: 
                    logging.error('Site %s does not have status in SSB' % voname) 
                    continue
                new_status = self.getState(str(status))
                if not new_status:
                    logging.error("Unkwown status '%s' for site %s, please check SSB" % (str(status), voname))
                    continue
                statusBySite[voname] = new_status
            else:
                logging.error('I have a duplicated status entry in SSB for %s' % voname) 
        return statusBySite

    def getState(self, stateFromSSB):
        """
        _getState_
        
        Translate SSB states into resource control state
        """
        if stateFromSSB == "on":
            return "Normal"
        elif stateFromSSB == "drain":
            return "Draining"
        elif stateFromSSB == "tier0":
            logging.debug('There is a site in tier0 status (Tier0Mode is %s)' % self.tier0Mode )
            if self.tier0Mode: 
                return "Normal"
            else:
                return "Draining"
        elif stateFromSSB == "down":
            return "Down"
        elif stateFromSSB == "skip":
            return "Down"
        else:
            return None

    def updateSiteInfo(self, siteName, state, CPUBound, IOBound, agentsNum):
        """
        _updateSiteInfo_
    
        Update information about a site in the database. Also set thresholds for a given site
        pending_jobs policy:
            sitePending is CPUBound*(pendingSlotsSitePercent/100)
            taskPending is (CPUBound or IOBound)*(pendingSlotsTaskPercent/100) depending on the task type
        This allows to maintain the right pressure in the queue, and keep the agent safe.
        The site threshold is higger than each task threshold. This allow to have different task type jobs in the queue.
        When there is several agents in the same team, we divide the pending threshold between the number of agents running.
        """
        if self.resourceControl.listSiteInfo(siteName) is None:
            logging.warn("Site %s has not been added to the resource control. Please check if the site was added by the condor plugin" % siteName)
            return False
        
        # set site state:
        self.resourceControl.changeSiteState(siteName, state)
        
        # tier0 T1 cores utilization
        if self.tier0Mode and 'T1_' in siteName:
            CPUBound = CPUBound*self.t1SitesCores/100
            IOBound = IOBound*self.t1SitesCores/100
        
        # Thresholds:
        sitePending = int(CPUBound/agentsNum*self.pendingSlotsSitePercent/100)
        taskCPUPending = int(CPUBound/agentsNum*self.pendingSlotsTaskPercent/100)
        taskIOPending = int(IOBound/agentsNum*self.pendingSlotsTaskPercent/100)
        
        # min pending values for thresholds
        if taskCPUPending < 10 and taskCPUPending > 0: 
            taskCPUPending = 10
        if taskIOPending < 10 and taskIOPending > 0: 
            taskIOPending = 10
        
        # Set site main thresholds
        self.resourceControl.setJobSlotsForSite(siteName = siteName,
                                                pendingJobSlots = sitePending,
                                                runningJobSlots = CPUBound)
        
        # Set thresholds for CPU bound task types
        cpuTasks = ['Processing', 'Production', 'Analysis']
        for task in cpuTasks:
            self.resourceControl.insertThreshold(siteName = siteName, taskType = task,
                                                 maxSlots = CPUBound, pendingSlots = taskCPUPending)
        
        # Set thresholds for IO bound task types
        ioTasks = ['Merge', 'Cleanup', 'Harvesting', 'LogCollect', 'Skim']
        for task in ioTasks:
            self.resourceControl.insertThreshold(siteName = siteName, taskType = task,
                                                 maxSlots = IOBound, pendingSlots = taskIOPending)
        
        if self.tier0Mode:
            # Set thresholds for tier0 task types
            expressSlots = int(CPUBound*self.runningExpressPercent/100)
            pendingExpress = int(expressSlots*self.pendingSlotsTaskPercent/100)
            self.resourceControl.insertThreshold(siteName = siteName, taskType = 'Express',
                                                 maxSlots = expressSlots, pendingSlots = pendingExpress)
            repackSlots = int(CPUBound*self.runningRepackPercent/100)
            pendingRepack = int(repackSlots*self.pendingSlotsTaskPercent/100)
            self.resourceControl.insertThreshold(siteName = siteName, taskType = 'Repack',
                                                 maxSlots = repackSlots, pendingSlots = pendingRepack)
        return True
