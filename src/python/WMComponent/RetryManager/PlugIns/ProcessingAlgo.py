#!/bin/env python


"""
_ProcessingAlgo_

'Smart' retry handler algorithm
"""
import os.path
import logging
import shutil
import cPickle
import threading
import time

from WMCore.DAOFactory import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMComponent.RetryManager.PlugIns.RetryAlgoBase import RetryAlgoBase


class ProcessingAlgo(RetryAlgoBase):
    """
    _ProcessingAlgo_

    'Smart' retry handler

    """

    def __init__(self, config):

        # Init basics
        RetryAlgoBase.__init__(self, config)

        myThread = threading.currentThread()

        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        self.getWorkflowsInjectedDAO = daoFactory(classname = "JobGroup.GetWorkflowInjected")
        self.getJobGroupUpdateTimeDAO = daoFactory(classname = "JobGroup.GetJobGroupUpdateTime")
        self.getSubscriptionJobCountsDAO = daoFactory(classname = "JobGroup.GetSubscriptionJobCounts")

        self.changeState = ChangeState(self.config)

        return

    def isReady(self, job, cooloffType):
        """
        Actual function that does the work

        """
        #
        # base coolOffTime (coolOffTime dict in configuration) in seconds
        #
        baseCoolOffTimeDict = self.getAlgoParam(job['jobType'])
        baseCoolOffTime = baseCoolOffTimeDict.get(cooloffType.lower(), 3600)

        #
        # the first retry for jobcooloff type jobs is usually shorter
        # default 1800 seconds or baseCoolOffTime, whatever is shorter
        # allowed is anything from 1 to baseCoolOffTime seconds
        #
        firstRetryCoolOffTime = self.getAlgoParam(job['jobType'], param = 'firstRetryCoolOffTime', defaultReturn = min(1800, baseCoolOffTime))
        if firstRetryCoolOffTime not in range(1, baseCoolOffTime + 1):
            logging.error("ERROR firstRetryCoolOffTime has to be an integer between 1 and %d, setting it to %d" % (baseCoolOffTime + 1, min(1800, baseCoolOffTime))) 
            firstRetryCoolOffTime = min(1800, baseCoolOffTime)

        #
        # growth of the coolOffTime with each retry (in percent of baseCoolOffTime)
        #
        #   coolOffTime = (1 + retry_count * coolOffTimeGrowth / 100) * baseCoolOffTime
        #
        # default is 100%, allowed are 0% to 200%
        #
        coolOffTimeGrowth = self.getAlgoParam(job['jobType'], param = 'coolOffTimeGrowth', defaultReturn = 100)
        if coolOffTimeGrowth not in range(201):
            logging.error("ERROR coolOffTimeGrowth has to be an integer between 0 and 200, setting it to 100") 
            coolOffTimeGrowth = 100

        #
        # maximum allowed cooloff time in seconds
        #
        # default is 12 hours or twice the baseCoolOffTime, whatever is longer
        # allowed is anything from baseCoolOffTime to either 24 hours or three times baseCoolOffTime (whatever is longer)
        #
        maxCoolOffTime = self.getAlgoParam(job['jobType'], param = 'maxCoolOffTime', defaultReturn = max(12*3600, 2 * baseCoolOffTime))
        if maxCoolOffTime not in range(baseCoolOffTime, max(24*3600, 3 * baseCoolOffTime)):
            logging.error("ERROR maxCoolOffTime has to be an integer between %d and %d, setting it to %d" %
                          (baseCoolOffTime, max(24*3600, 3 * baseCoolOffTime), max(12*3600, 2 * baseCoolOffTime)))
            maxCoolOffTime = max(12*3600, 2 * baseCoolOffTime)

        #
        # number of retries we always do, even in closeout
        # also the retries during which we can modify the site blacklist
        #
        # default is 4, allowed are 1 to 99
        #
        guaranteedRetries = self.getAlgoParam(job['jobType'], param = 'guaranteedRetries', defaultReturn = 4)
        if guaranteedRetries not in range(1,100):
            logging.error("ERROR guaranteedRetries has to be an integer between 1 and 99, setting it to 4")
            guaranteedRetries = 4

        #
        # completion percentage for subscription that triggers closeout
        #
        # default is 95% , allowed are 50% to 100% 
        #
        closeoutPercentage = self.getAlgoParam(job['jobType'], param = 'closeoutPercentage', defaultReturn = 95)
        if closeoutPercentage not in range(50,101):
            logging.error("ERROR closeoutPercentage has to be an integer between 50 and 100, setting it to 95")
            closeoutPercentage = 95

        #
        # maximum amout of time a job can be retried in hours
        #  (only applies if workflow is injected)
        #
        # default is 72 hours, allowed are between 24 and 168 hours
        #
        maxRetryTime = self.getAlgoParam(job['jobType'], param = 'maxRetryTime', defaultReturn = 72)
        if maxRetryTime not in range(2, 169):
            logging.error("ERROR maxRetryTime has to be an integer between 2 and 168, setting it to 72")
            maxRetryTime = 72

        #
        # minimum amount of time in hours a workflow should be retried before being allowed to fail out
        #  (only applies if workflow is injected)
        #
        # default is 1 hour, allowed are between 1 and 168 hours
        #
        minRetryTime = self.getAlgoParam(job['jobType'], param = 'minRetryTime', defaultReturn = 1)
        if minRetryTime not in range(1, 169):
            logging.error("ERROR minRetryTime has to be an integer between 1 and 169, setting it to 1")
            minRetryTime = 1
        if minRetryTime > maxRetryTime:
            logging.error("ERROR minRetryTime can't be larger than maxRetryTime, setting it to maxRetryTime")
            minRetryTime = maxRetryTime

        #
        # stop immediately for create failures
        #
        if cooloffType == 'create':
            self.stopRetrying(job)
            return False

        #
        # calculate timeout
        #
        if job['retry_count'] == 0:
            timeout = firstRetryCoolOffTime
        elif job['retry_count'] in range(guaranteedRetries):
            timeout = min((1 + ( job['retry_count'] - 1) * coolOffTimeGrowth / 100) * baseCoolOffTime, maxCoolOffTime)
        else:
            timeout = maxCoolOffTime

        #
        # don't modify the site blacklist for submit failures
        #
        if cooloffType == 'submit':
            if job['retry_count'] >= guaranteedRetries:
                if self.inCloseout(job, minRetryTime, maxRetryTime, closeoutPercentage):
                    self.stopRetrying(job)
                    return False
            return self.retryWithTimeout(job, timeout)

        #
        # different behavior for different retries
        #
        if job['retry_count'] in range(guaranteedRetries):
            if self.retryWithTimeout(job, timeout):
                self.modifySiteBlacklist(job, False)
                return True
            else:
                return False
        else:
            if self.inCloseout(job, minRetryTime, maxRetryTime, closeoutPercentage):
                    self.stopRetrying(job)
                    return False
            elif self.retryWithTimeout(job, maxCoolOffTime):
                self.modifySiteBlacklist(job, True)
                return True
            else:
                return False

    def inCloseout(self, job, minRetryTime, maxRetryTime, closeoutPercentage):
        """
        Helper function that determines if the workflow
        associated with the jobgroup is in closeout

        """
        if self.getWorkflowsInjectedDAO.execute(jobgroup = job['jobgroup']):
            updateTime = self.getJobGroupUpdateTimeDAO.execute(jobgroup = job['jobgroup'])
            totalRetryTime = self.timestamp() - updateTime
            if totalRetryTime < minRetryTime * 3600:
                logging.info("DEBUG minRetryTime not reached yet, continue")
                return False
            elif totalRetryTime > maxRetryTime * 3600:
                logging.info("DEBUG maxRetryTime reached, stop")
                return True
            else:
                (allJobs, completeJobs) = self.getSubscriptionJobCountsDAO.execute(jobgroup = job['jobgroup'])
                logging.info("DEBUG finally hitting closeout")
                return ( completeJobs*100 / allJobs >= closeoutPercentage )
        else:
            return False

    def modifySiteBlacklist(self, job, resetFlag):
        """
        Helper function that modifies the site blacklist
        based on current failure locations

        """
        pickledJobPath = os.path.join(job['cache_dir'], "job.pkl")
        pickledJobPathBackup = os.path.join(job['cache_dir'], "job.pkl.backup")

        # This should never happen, don't do anything
        if not os.path.isfile(pickledJobPath):
            return

        if job['retry_count'] == 0:
            logging.info("DEBUG backup pickled job file for job %s" % job['id'])
            shutil.copy2(pickledJobPath, pickledJobPathBackup)
        elif resetFlag:
            if os.path.isfile(pickledJobPathBackup):
                logging.info("DEBUG restore/move pickled job file backup for job %s" % job['id'])
                shutil.move(pickledJobPathBackup, pickledJobPath)
            return
        else:
            if not os.path.isfile(pickledJobPathBackup):
                logging.info("DEBUG no pickled job file backup for job %s" % job['id'])
                return

        try:
            jobHandle = open(pickledJobPath, "r")
            loadedJob = cPickle.load(jobHandle)
            jobHandle.close()
        except Exception, ex:
            # just silently ignore
            return

        logging.info("DEBUG siteWhiteList %s for job %s" % (loadedJob['siteWhitelist'], job['id']))
        logging.info("DEBUG siteBlackList %s for job %s" % (loadedJob['siteBlacklist'], job['id']))

        tempList = []
        for site in loadedJob['siteWhitelist']:
            if site not in loadedJob['siteBlacklist']:
                tempList.append(site)

        logging.info("DEBUG tempList %s for job %s" % (tempList, job['id']))

        if len(tempList) > 1:
            if job['location'] in tempList:
                loadedJob['siteBlacklist'].append(job['location'])
                try:
                    jobHandle = open(pickledJobPath, "w")
                    cPickle.dump(loadedJob, jobHandle)
                    jobHandle.close()
                except Exception, ex:
                    # could have corrupted job file, use backup
                    logging.info("DEBUG restore/copy pickled job file backup for job %s" % job['id'])
                    shutil.copy2(pickledJobPathBackup, pickledJobPath)
        else:
            logging.info("DEBUG restore/move pickled job file backup for job %s" % job['id'])
            shutil.move(pickledJobPathBackup, pickledJobPath)

        return

    def retryWithTimeout(self, job, timeout):
        """
        Helper function that retries job after fixed timeout

        """
        return self.timestamp() - job['state_time'] > timeout

    def stopRetrying(self, job):
        """
        Helper function to stop retrying

        """
        self.changeState.propagate(job, "retrydone", job['state'])
        return
