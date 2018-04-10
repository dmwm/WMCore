"""
Thread used to monitor the agent draining process. Should
eventually report no issues meaning that the agent is ready
to be shutdown and a new version be put in place.
"""
from __future__ import division

__all__ = []

import logging
from Utils.Timers import timeFunction
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.ReqMgrAux.ReqMgrAux import isDrainMode, ReqMgrAux
from WMComponent.AgentStatusWatcher.DrainStatusAPI import DrainStatusAPI
from WMCore.Services.PyCondor.PyCondorAPI import PyCondorAPI

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
        self.drainAPI = DrainStatusAPI()
        self.condorAPI = PyCondorAPI()
        self.speedDrainConfig = None

        if hasattr(config, "Tier0Feeder"):
            self.reqAuxDB = None
        else:
            self.reqAuxDB = ReqMgrAux(self.config.General.ReqMgr2ServiceURL)

    @timeFunction
    def algorithm(self, parameters):
        """
        Update drainStats if agent is in drain mode
        """
        if isDrainMode(self.config):
            logging.info("Checking agent drain status...")
            # check to see if the agent hit any speed drain thresholds
            if self.reqAuxDB:
                logging.info("Checking speed drain thresholds...")
                thresholdsHit = self.checkSpeedDrainThresholds()
                if thresholdsHit:
                    logging.info("Updating agent configuration for speed drain...")
                    self.updateAgentSpeedDrainConfig(thresholdsHit)
            try:
                DrainStatusPoller.drainStats = self.drainAPI.collectDrainInfo()
                logging.info("Finished collecting agent drain status.")
                logging.info("Drain stats: " + str(DrainStatusPoller.drainStats))

            except Exception as ex:
                msg = "Error occurred, will retry later:\n"
                msg += str(ex)
                logging.exception(msg)
        else:
            logging.info("Agent not in drain mode. Skipping drain check...")

    @classmethod
    def getDrainInfo(cls):
        """
        Return drainStats class variable
        """
        return cls.drainStats

    def updateAgentSpeedDrainConfig(self, thresholdsHit):
        """
        Takes a list of speed drain configuration keys and updates the agent configuration
        """
        updateConfig = False

        if 'CondorPriority' in thresholdsHit:
            if self.condorAPI.editCondorJobs(
                    "JobStatus=?=1 && (CMS_JobType =?= \"Production\" || CMS_JobType =?= \"Processing\")",
                    "JobPrio", "999999"):
                logging.info("Condor job priority updated to 999999 for pending Production/Processing jobs.")
                if not self.speedDrainConfig['CondorPriority']['Enabled']:
                    self.speedDrainConfig['CondorPriority']['Enabled'] = True
                    updateConfig = True

        if 'NoJobRetries' in thresholdsHit:
            if self.reqAuxDB.updateAgentConfig(self.config.Agent.hostName, "MaxRetries", 0):
                logging.info("Agent updated max retries to 0.")
                self.speedDrainConfig['NoJobRetries']['Enabled'] = True
                updateConfig = True

        if 'EnableAllSites' in thresholdsHit:
            logging.info("Updating agent to submit to all sites.")
            # setting this value to True makes JobSubmitterPoller ignore site status
            self.speedDrainConfig['EnableAllSites']['Enabled'] = True
            updateConfig = True

        # update the aux db speed drain config with any changes
        if updateConfig:
            self.reqAuxDB.updateAgentConfig(self.config.Agent.hostName, "SpeedDrainMode", True)
            self.reqAuxDB.updateAgentConfig(self.config.Agent.hostName, "SpeedDrainConfig", self.speedDrainConfig)

        return

    def checkSpeedDrainThresholds(self):
        """
        Check the current number of jobs in Condor and create a list of agent configuration parameters
        that need updated for speed draining
        """
        validKeys = ['CondorPriority', 'NoJobRetries', 'EnableAllSites']
        enableKeys = []

        # get the current speed drain status
        self.speedDrainConfig = self.reqAuxDB.getWMAgentConfig(self.config.Agent.hostName).get("SpeedDrainConfig")

        # get condor jobs
        jobs = self.condorAPI.getCondorJobs("", [])
        if jobs is None:
            logging.warning("There was an error querying the schedd.  Not checking speed drain thresholds.")
            return []

        # loop through the speed drain configuration and make a list of what thresholds have been hit
        for k, v in self.speedDrainConfig.items():
            # make sure keys in the speed drain config are valid
            if k in validKeys and isinstance(v['Threshold'], int) and isinstance(v['Enabled'], bool):
                # we always want to apply the condor priority change if the threshold is hit
                if not v['Enabled'] or k == 'CondorPriority':
                    logging.info("Checking speed drain threshold for %s. ", k)
                    if len(jobs) < v['Threshold']:
                        logging.info("Agent will update speed drain configuration for %s. ", k)
                        enableKeys.append(k)
            else:
                logging.warning("Speed drain configuration error for %s.  Please check aux db contents.", k)

        return enableKeys