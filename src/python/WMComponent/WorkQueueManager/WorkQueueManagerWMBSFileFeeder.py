#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []




import time
import random

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

class WorkQueueManagerWMBSFileFeeder(BaseWorkerThread):
    """
    Polls for Work
    """
    def __init__(self, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.queue = queue
        self.config = config
        self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
        # state lists which shouldn't be populated in wmbs. (To prevent creating work before WQE status updated)
        self.abortedAndForceCompleteWorkflowCache = self.reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache()


    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        t = random.randrange(self.idleTime)
        self.logger.info('Sleeping for %d seconds before 1st loop' % t)
        time.sleep(t)

    def algorithm(self, parameters):
        """
        Pull in work
        """
        try:
            self.getWorks()
        except Exception as ex:
            self.queue.logger.error("Error in wmbs inject loop: %s" % str(ex))

    def getWorks(self):
        """
        Inject work into wmbs for idle sites
        """
        self.queue.logger.info("Getting work and feeding WMBS files")

        # need to make sure jobs are created
        resources, jobCounts = freeSlots(minusRunning = True, allowedStates = ['Normal', 'Draining'],
                              knownCmsSites = cmsSiteNames())

        for site in resources:
            self.queue.logger.info("I need %d jobs on site %s" % (resources[site], site))

        abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()

        tempACDCExceptions = ["mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053538_530",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053544_9947",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053551_9974",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053600_3305",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053608_5854",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053617_8169",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053625_8322",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053632_9709",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053641_9747",
                              "mcremone_ACDC0_task_EGM-RunIISummer17GS-00002__v1_T_170924_053649_646",
                              "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_054940_8115",
                            # prebello_Run2016G-v1-SingleMuon-07Aug17_8029_170811_175009_9489
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_054953_3843",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055006_2236",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055016_487",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055027_1392",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055034_1805",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055041_5982",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055048_4811",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055055_4038",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055102_5520",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055110_2575",
                            "mcremone_ACDC0_Run2016G-v1-SingleMuon-07Aug17_8029_170924_055117_4664",
                            # fabozzi_Run2016F-v1-MuonEG-07Aug17_8029_170831_192544_4761
                            "mcremone_ACDC0_Run2016F-v1-MuonEG-07Aug17_8029_170924_054427_8461",
                            # fabozzi_Run2016G-v1-HTMHT-07Aug17_8029_170831_185809_149
                            "mcremone_ACDC0_Run2016G-v1-HTMHT-07Aug17_8029_170924_054805_2608",
                            ]
        abortedAndForceCompleteRequests.extend(tempACDCExceptions)
        self.queue.logger.info("Excluding following Workflows %s", abortedAndForceCompleteRequests)
        previousWorkList = self.queue.getWork(resources, jobCounts, excludeWorkflows=abortedAndForceCompleteRequests)
        self.queue.logger.info("%s of units of work acquired for file creation"
                               % len(previousWorkList))
        return
