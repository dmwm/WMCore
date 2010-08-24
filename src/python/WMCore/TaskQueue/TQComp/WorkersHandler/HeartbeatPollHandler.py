#!/usr/bin/env python

"""
_HeartbeatPollHandler_



"""





#import os
#import logging
#import inspect
#import time
import sys
import threading
from traceback import extract_tb

from WMCore.WMFactory import WMFactory
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from TQComp.Defaults import heartbeatValidity
from TQComp.TQRoutines import finishPilot


class HeartbeatPollHandler(BaseWorkerThread):
    """ 
    _HeartbeatPollHandler_ 
    
    """
    def __init__(self):
        BaseWorkerThread.__init__(self)
      
	
    def setup(self, parameters):
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                  "TQComp.Database."+myThread.dialect)
        self.queries = factory.loadObject("Queries")
        self.logger = myThread.logger
      
    
    def algorithm(self, parameters):
        myThread = threading.currentThread()

        if parameters['hbValidity'] != None:
            hbValidity = parameters['hbValidity']
        else:
            hbValidity = heartbeatValidity

        res1 = res2 = []
        try:
            myThread.transaction.begin()

            # Retrieve pilots that have lived too long
            resTtl = self.queries.checkPilotsTtl()
            self.logger.debug("Too long-lived pilots: %s" % resTtl)

            # Retrieve pilots that have not reported for too long
            resHbt = self.queries.checkPilotsHeartbeat(hbValidity)
            self.logger.debug("Too careless pilots: %s" % resHbt)

            # Commit
            myThread.transaction.commit()

        except:
            ttype, val, tb = sys.exc_info()
            myThread.transaction.rollback()
            messg = 'Error in HeartbeatPollHandler: %s - %s '% (ttype, val)
            self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))


        # TODO: Do nothing with pilots that lived too long?
        #       If they don't commit suicide or get killed, why not use them?
#        for pilot in resTtl:
#            finishPilot(self, myThread.transaction, \
#                                   pilot[0], 'TtlDeath')
                                   
        # Terminate those pilots that did not report (discard output)
        for pilot in resHbt:
            finishPilot(self, myThread.transaction, \
                                   pilot[0], 'HeartbeatDeath')

        return

