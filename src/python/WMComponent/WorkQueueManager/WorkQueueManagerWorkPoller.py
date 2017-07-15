#!/usr/bin/env python
"""
_WorkQueueManagerPoller_

Pull work out of the work queue.
"""




import time
import random
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Agent.Configuration import loadConfigurationFile
from WMComponent.AnalyticsDataCollector.DataCollectAPI import isDrainMode

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
        # refresh agent configuration, config file is automatically fetched
        self.config = loadConfigurationFile(None)
        self.pullWork()

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
        set true or false for given retrieve condition
        i.e. thresholds on workqueue, agent in drain
        """
        return not isDrainMode(self.config)

    def pullWork(self):
        """Get work from parent"""
        if not self.passRetrieveCondition():
            self.queue.logger.info("Draining queue: skipping work pull")
            return

        try:
            self.queue.logger.info("Pulling work from %s" % self.queue.parent_queue.queueUrl)
            work = self.queue.pullWork()
            self.queue.logger.info("Obtained %s unit(s) of work" % work)
        except IOError as ex:
            self.queue.logger.error("Error opening connection to work queue: %s \n%s" %
                                    (str(ex), traceback.format_exc()))
        except Exception as ex:
            self.queue.logger.error("Unable to pull work from parent Error: %s\n%s"
                                    % (str(ex), traceback.format_exc()))
        return

    def processWork(self):
        """Process new work"""
        self.queue.logger.info("Splitting new work")
        try:
            self.queue.processInboundWork()
        except Exception as ex:
            self.queue.logger.exception('Error during split')
        self.logger.info('Splitting finished')
        return
