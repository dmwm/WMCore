#!/usr/bin/env python
"""
_WorkQueueManagerPoller_

Pull work out of the work queue.
"""




import time
import random
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

class WorkQueueManagerWorkPoller(BaseWorkerThread):
    """
    Polls for Work
    """
    def __init__(self, queue):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue

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
        except Exception, ex:
            self.queue.logger.error("Error in work pull loop: %s" % str(ex))
        try:
            # process if we get work or not - we may have to split old work
            # i.e. if transient errors were seen during splitting
            self.processWork()
        except Exception, ex:
            self.queue.logger.error("Error in new work split loop: %s" % str(ex))
        return

    def retrieveCondition(self):
        """
        _retrieveCondition_
        set true or false for given retrieve condion
        i.e. thredshod on workqueue
        """
        return True

    def pullWork(self):
        """Get work from parent"""
        self.queue.logger.info("Pulling work from %s" % self.queue.parent_queue.queueUrl)
        work = 0
        try:
            if self.retrieveCondition():
                work = self.queue.pullWork()
        except IOError, ex:
            self.queue.logger.error("Error opening connection to work queue: %s \n%s" % 
                                    (str(ex), traceback.format_exc()))
        except StandardError, ex:
            self.queue.logger.error("Unable to pull work from parent Error: %s\n%s"
                                    % (str(ex), traceback.format_exc()))
        self.queue.logger.info("Obtained %s unit(s) of work" % work)
        return work

    def processWork(self):
        """Process new work"""
        self.queue.logger.info("Splitting new work")
        try:
            self.queue.processInboundWork()
        except StandardError, ex:
            self.queue.logger.exception('Error during split')
        self.logger.info('Splitting finished')
        return
