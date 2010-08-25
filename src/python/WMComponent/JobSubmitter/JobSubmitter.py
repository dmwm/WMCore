#!/usr/bin/env python
#pylint: disable-msg=W6501
# W6501: pass information to logging using string arguments

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitter.py,v 1.3 2010/02/12 21:14:45 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

import logging
import threading

from WMCore.Agent.Harness     import Harness

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller


class JobSubmitter(Harness):
    """
    Creates jobs for new subscriptions

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        logging.info("JobSubmitter.__init__")

    def preInitialization(self):
        """
        Setup the worker thread for jobSubmitter

        """
        logging.info("JobSubmitter.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()
        
        pollInterval = self.config.JobSubmitter.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobSubmitterPoller(self.config),
                                               pollInterval)

        return
