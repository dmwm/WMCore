"""
Thread used to monitor the agent draining process. Should
eventually report no issues meaning that the agent is ready
to be shutdown and a new version be put in place.
"""
from __future__ import division

from future.utils import viewitems

__all__ = []

import logging
import copy
from Utils.Timers import timeFunction
from WMComponent.AgentStatusWatcher.DrainStatusAPI import DrainStatusAPI
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.PyCondor.PyCondorAPI import PyCondorAPI
from Utils.EmailAlert import EmailAlert

class DrainStatusPoller(BaseWorkerThread):
    """
    Collects information related to the agent drain status
    """
    # class variable that contains drain statistics
    drainStats = {}

    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        self.config = config
        self.drainAPI = DrainStatusAPI(config)
        self.condorAPI = PyCondorAPI()
        self.agentConfig = {}
        self.previousConfig = {}
        self.validSpeedDrainConfigKeys = ['CondorPriority', 'NoJobRetries', 'EnableAllSites']
        cacheduration = getattr(self.config.General, "ReqMgrAuxCacheDuration", 5 / 60)  # 5 minutes
        self.reqAuxDB = ReqMgrAux(self.config.General.ReqMgr2ServiceURL, httpDict={'cacheduration': cacheduration})
        self.emailAlert = EmailAlert(config.EmailAlert.dictionary_())
        self.condorStates = ("Running", "Idle")

    @timeFunction
    def algorithm(self, parameters):
        """
        Update drainStats if agent is in drain mode
        """
        logging.info("Running agent drain algorithm...")
        if self.agentConfig:
            # make a copy of the previous agent aux db configuration to compare against later
            self.previousConfig = copy.deepcopy(self.agentConfig)
        # grab a new copy of the agent aux db configuration
        self.agentConfig = self.reqAuxDB.getWMAgentConfig(self.config.Agent.hostName)
        if not self.agentConfig:
            logging.error("Failed to fetch agent configuration from the auxiliary DB")
            return

        try:
            # see if the agent is in drain mode
            if self.agentConfig["UserDrainMode"] or self.agentConfig["AgentDrainMode"]:
                # check to see if the agent hit any speed drain thresholds
                thresholdsHit = self.checkSpeedDrainThresholds()
                if thresholdsHit:
                    logging.info("Updating agent configuration for speed drain...")
                    self.updateAgentSpeedDrainConfig(thresholdsHit)
                # now collect drain statistics
                DrainStatusPoller.drainStats = self.drainAPI.collectDrainInfo()
                logging.info("Finished collecting agent drain status.")
                logging.info("Drain stats: %s", str(DrainStatusPoller.drainStats))
            else:
                logging.info("Agent not in drain mode. Resetting flags and skipping drain check...")
                self.resetAgentSpeedDrainConfig()

            # finally, check for any changes in drain status
            self.checkDrainStatusChanges()

        except Exception as ex:
            msg = "Error occurred, will retry later:\n"
            msg += str(ex)
            logging.exception(msg)

    @classmethod
    def getDrainInfo(cls):
        """
        Return drainStats class variable
        """
        return cls.drainStats

    def checkDrainStatusChanges(self):
        """
        Check to see if any drain statuses have changed in the auxiliary db
        If yes, send email notification and update local drain thread variables

        """
        message = ""
        drainStatusKeys = ['UserDrainMode', 'AgentDrainMode', 'SpeedDrainMode']

        if not self.previousConfig:
            return

        for key in drainStatusKeys:
            if self.previousConfig[key] != self.agentConfig[key]:
                message += "Agent had a drain status transition to %s = %s\n" % (str(key), str(self.agentConfig[key]))

        if message:
            self.emailAlert.send("DrainMode status change on " + getattr(self.config.Agent, "hostName"), message)
            logging.info("Drain mode status change: %s", message)

        return

    def updateAgentSpeedDrainConfig(self, thresholdsHit):
        """
        Takes a list of speed drain configuration keys and updates the agent configuration
        """
        updateConfig = False
        condorPriorityFlag = False
        speedDrainConfig = self.agentConfig.get("SpeedDrainConfig")

        if 'CondorPriority' in thresholdsHit:
            logging.info("Bumping condor job priority to 999999 for Production/Processing pending jobs.")
            self.condorAPI.editCondorJobs(
                "JobStatus=?=1 && (CMS_JobType =?= \"Production\" || CMS_JobType =?= \"Processing\")",
                "JobPrio", "999999")
            condorPriorityFlag = True

        if condorPriorityFlag != speedDrainConfig['CondorPriority']['Enabled']:
            # CondorPriority setting is irreversible so the flag only indicates weather
            # priority is increased or not. It is not checked by other components
            logging.info("Enabling CondorPriority flag.")
            speedDrainConfig['CondorPriority']['Enabled'] = condorPriorityFlag
            updateConfig = True

        if 'NoJobRetries' in thresholdsHit:
            logging.info("Enabling NoJobRetries flag: Error Handler won't retry the jobs")
            # ErrorHandler will pick this up and set max retries to 0
            speedDrainConfig['NoJobRetries']['Enabled'] = True
            updateConfig = True

        if 'EnableAllSites' in thresholdsHit:
            logging.info("Enabling EnableAllSites flag: Updating agent to submit to all sites.")
            # setting this value to True makes JobSubmitterPoller ignore site status
            speedDrainConfig['EnableAllSites']['Enabled'] = True
            updateConfig = True

        # update the aux db speed drain config with any changes
        if updateConfig:
            self.agentConfig['SpeedDrainMode'] = True
            self.reqAuxDB.updateWMAgentConfig(self.config.Agent.hostName, self.agentConfig)

        return

    def resetAgentSpeedDrainConfig(self):
        """
        resetting SpeedDrainMode to False and SpeedDrainConfig Enabled to False
        """

        if self.agentConfig.get("SpeedDrainMode"):
            self.agentConfig['SpeedDrainMode'] = False
            speedDrainConfig = self.agentConfig.get("SpeedDrainConfig")
            for key, v in viewitems(speedDrainConfig):
                if key in self.validSpeedDrainConfigKeys and v['Enabled']:
                    speedDrainConfig[key]['Enabled'] = False

            self.reqAuxDB.updateWMAgentConfig(self.config.Agent.hostName, self.agentConfig)
        return

    def checkSpeedDrainThresholds(self):
        """
        Check the current number of jobs in Condor and create a list of agent configuration parameters
        that need updated for speed draining
        """
        enableKeys = []
        # first, update our summary of condor jobs
        totalJobs = self.getTotalCondorJobs()
        if totalJobs is None:
            msg = "Cannot check speed drain because there was an error fetching job summary from HTCondor."
            msg += " Will retry again in the next cycle."
            logging.warning(msg)
            return []

        # get the current speed drain status
        speedDrainConfig = self.agentConfig.get("SpeedDrainConfig")

        # loop through the speed drain configuration and make a list of what thresholds have been hit
        for k, v in viewitems(speedDrainConfig):
            # make sure keys in the speed drain config are valid
            if k in self.validSpeedDrainConfigKeys and isinstance(v['Threshold'], int) and isinstance(v['Enabled'], bool):
                # we always want to apply the condor priority change if the threshold is hit
                if not v['Enabled'] or k == 'CondorPriority':
                    logging.info("Checking speed drain threshold for %s. ", k)
                    if totalJobs < v['Threshold']:
                        logging.info("Agent will update speed drain configuration for %s. ", k)
                        enableKeys.append(k)
            else:
                logging.warning("Speed drain configuration error for %s.  Please check aux db contents.", k)

        return enableKeys

    def getTotalCondorJobs(self):
        """
        Retrieve a summary of the jobs in condor and return an absolute number
        of the jobs in Idle and Running states.
        :return: returns an integer with the total number of jobs, or None if it failed.
        """
        jobs = self.condorAPI.getCondorJobsSummary()
        if not jobs:
            return None

        results = 0
        if jobs:
            for state in self.condorStates:
                results += int(jobs[0].get(state))
        return results
