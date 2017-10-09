#!/usr/bin/env python
"""
_WorkQueueManagerPoller_

Pull work out of the work queue.
"""

import time
import random
import traceback
import threading

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.PyCondor.PyCondorAPI import PyCondorAPI
from WMCore.Services.ReqMgrAux.ReqMgrAux import isDrainMode

class WorkQueueManagerWorkPoller(BaseWorkerThread):
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
        self.condorAPI = PyCondorAPI()

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
            self.pullWork()
        except Exception as ex:
            self.queue.logger.error("Error in work pull loop: %s" % str(ex))
        try:
            # process if we get work or not - we may have to split old work
            # i.e. if transient errors were seen during splitting
            self.processWork()
        except Exception as ex:
            self.queue.logger.error("Error in new work split loop: %s" % str(ex))
        return

    def passRetrieveCondition(self):
        """
        _passRetrieveCondition_
        
        Return true if the component can proceed with fetching work.
        False if the component should skip pulling work this cycle.
        
        For now, it only checks whether the agent is in drain mode or
        if the condor schedd is overloaded.
        """
        passCond = True
        if isDrainMode(self.config):
            passCond = False
        elif self.condorAPI.isScheddOverloaded():
            passCond = False

        return passCond

    def pullWork(self):
        """Get work from parent"""
        self.queue.logger.info("Pulling work from %s" % self.queue.parent_queue.queueUrl)
        work = 0

        myThread = threading.currentThread()

        try:
            if self.passRetrieveCondition():
                work = self.queue.pullWork()
                myThread.logdbClient.delete("LocalWorkQueue_pullWork", "warning", this_thread=True)
            else:
                msg = "Workqueue didn't pass the retrieve condition: NOT pulling work"
                self.queue.logger.warning(msg)
                myThread.logdbClient.post("LocalWorkQueue_pullWork", msg, "warning")
        except IOError as ex:
            self.queue.logger.error("Error opening connection to work queue: %s \n%s" %
                                    (str(ex), traceback.format_exc()))
        except Exception as ex:
            self.queue.logger.error("Unable to pull work from parent Error: %s\n%s"
                                    % (str(ex), traceback.format_exc()))
        self.queue.logger.info("Obtained %s unit(s) of work" % work)
        return work

    def processWork(self):
        """Process new work"""
        self.queue.logger.info("Splitting new work")
        try:
            self.queue.processInboundWork()
        except Exception as ex:
            self.queue.logger.exception('Error during split: %s' % str(ex))
        self.logger.info('Splitting finished')
        return
