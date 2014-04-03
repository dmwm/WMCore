"""
Perform cleanup actions
"""
__all__ = []

import urllib,urllib2, re
import threading
import logging
import traceback
import json
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

class ResourcesUpdate(BaseWorkerThread):
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
        
        # set phedex connection
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        
        # get dashboard url, set metric columns from config
        self.dashboard = config.ResourceHandling.dashboard
        self.siteStatusMetric = config.ResourceHandling.siteStatusMetric
        self.cpuBoundMetric = config.ResourceHandling.cpuBoundMetric
        self.ioBoundMetric = config.ResourceHandling.ioBoundMetric
        
        # set pending percentages from config
        self.pendingSlotsSitePercent = config.ResourceHandling.pendingSlotsSitePercent
        self.pendingSlotsTaskPercent = config.ResourceHandling.pendingSlotsTaskPercent
            
    def setup(self, parameters):
        """
        Set db connection and prepare resource control
        """
        # Interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set resource control
        self.resourceControl = ResourceControl(config = self.config)
        
        # create PhEDEx node mapping
        self.nodeNames = []
        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:
            if node["kind"] == 'Disk':
                self.nodeNames.append(node["name"].replace('_Disk',''))

    def algorithm(self, parameters):
        """
        _algorithm_
        
        Update site info about state and thresholds
            1. Get information from SSB
            2. Set site status and set therholds for each valid site
        Sites from SSB are validated with PhEDEx node names
        """
        try:
            logging.info("Getting site info from SSB")
            stateBySite, slotsCPU, slotsIO = self.getInfoFromSSB()
            
            logging.info("Setting states and thresholds for all sites")
            for site in stateBySite.keys():
                ##fix state to understandable
                if site in self.nodeNames:
                    if stateBySite[site] == 'skip':
                        self.updateSiteInfo(site, "Down", 0, 0)
                        
                        logging.debug('Setting state Down for skiped site %s' % site)
                        logging.debug('Setting thresholds for site %s: CPUBound = 0, IOBound = 0' % site)
                        
                    elif stateBySite[site] in ['down', 'on', 'drain']:
                        sitestate = self.getState(stateBySite[site])
                        self.updateSiteInfo(site, sitestate, slotsCPU[site], slotsIO[site])
                        
                        logging.debug('Setting state %s for site %s' % (sitestate, site))
                        logging.debug('Setting thresholds for site %s: CPUBound = %s, IOBound = %s' % (site, slotsCPU[site], slotsIO[site]))

                    else:
                        logging.error("Unkwown status '%s' for site %s" % (stateBySite[site], site))
                else:
                    logging.error("Site '%s' is not in PhEDEx" % site)
            
            logging.info("Resource update is completed.")
            
        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())

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
                    logging.debug('Site %s does not have threholds in SSB' % voname) 
                    continue
                thresholdbyVOName[voname] = int(value)
            else:
                logging.debug('I have a duplicated threshold entry in SSB for %s' % voname) 
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
                if status is None: 
                    logging.debug('Site %s does not have status in SSB' % voname) 
                    continue
                statusBySite[voname] = str(status)
            else:
                logging.debug('I have a duplicated status entry in SSB for %s' % voname) 
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
        elif stateFromSSB == "down":
            return "Down"

    def updateSiteInfo(self, siteName, state, CPUBound, IOBound):
        """
        _updateSiteInfo_
    
        Update information about a site in the database. Also set thresholds for a given site
        pending_jobs policy:
            sitePending is CPUBound*(pendingSlotsSitePercent/100)
            taskPending is (CPUBound or IOBound)*(pendingSlotsTaskPercent/100) depending on the task type
        This allows to maintain the right pressure in the queue, and keep the agent safe.
        The site threshold is higger than each task threshold. This allow to have different task type jobs in the queue.
        """
        if self.resourceControl.listSiteInfo(siteName) is None:
            logging.error("Site %s has not been added to the database. Please check if the site was added by the condor plugin" % siteName)
            return
        
        # set site state:
        self.resourceControl.changeSiteState(siteName, state)
        
        # Thresholds:
        sitePending = int(CPUBound*self.pendingSlotsSitePercent/100)
        taskCPUPending = int(CPUBound*self.pendingSlotsTaskPercent/100)
        taskIOPending = int(IOBound*self.pendingSlotsTaskPercent/100)
        
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
        for task in cpuTasks:
            self.resourceControl.insertThreshold(siteName = siteName, taskType = task,
                                                 maxSlots = IOBound, pendingSlots = taskIOPending)
        
        return