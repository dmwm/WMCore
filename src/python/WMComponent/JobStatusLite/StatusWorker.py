#!/usr/bin/env python
"""
The JobStatus subprocess worker for the status check/update
"""
__all__ = []
__revision__ = "$Id: StatusWorker.py,v 1.2 2010/05/18 07:31:01 mcinquil Exp $"
__version__ = "$Revision: 1.2 $"

import threading
import logging
import time

#from WMCore.DAOFactory                      import DAOFactory
from WMCore.Database.Transaction            import Transaction
from WMCore.Agent.Configuration             import Configuration
from WMComponent.JobStatusLite.JobStatusWork    import JobStatusWork


class StatusWorker:
    """
    The JobStatus subprocess worker for the status check/update
    """

    def __init__(self, **configDict):
        """
        init StatusWorker
        """

        myThread = threading.currentThread()
#        self.transaction = myThread.transaction

        #DAO factory for WMBS objects
        #self.daoFactory = DAOFactory( \
#                                      package = "WMCore.WMBS", \
#                                      logger = logging, \
#                                      dbinterface = myThread.dbi \
#                                    )

        config = Configuration()
        self.config = config

        logging.debug(str(myThread) + ": in StatusWorker.__init__")

        return

    def checkStatusByGroup(self, group):
        """
        _checkStatusByGroup_

        call the status check method per each group
        """
        logging.debug("in StatusWorker.checkStatusByGroup")
        try:
            from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
            from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched
            JobStatusWork.doWork(group)
        except ImportError, ie:
            logging.error("Problem importing BossLiteAPI. Simulating status check.")
            sleepmax = 3
            sleepspl = 0.1
            sleeptot = 0

            while(sleeptot < sleepmax):
                time.sleep(sleepspl)
                sleeptot += sleepspl

        except Exception, ex:
            logging.error("Problem checking status: '%s'"%str(ex))


    def __call__(self, parameters):
        """
        __call__

        processing input groups
        """
  
        myThread = threading.currentThread()

        logging.debug(str(myThread) + ": in StatusWorker.__call__")
        logging.info("Params: " + str(parameters) )

        logging.debug(str(myThread) + ": config " + str(self.config))

        if type(parameters) == type(dict()):
            for k in parameters.keys():
                logging.info("Received group " + str(k))
                self.checkStatusByGroup(k)
        else:
            for param in parameters:
                for k in param.keys():
                    logging.info("Received group " + str(k))
                    self.checkStatusByGroup(k)

        logging.info(str(myThread) + ": finished StatusWorker.__call__")
        logging.info(str(myThread) + ": returning [%s]"%str(parameters))
        
        return parameters

