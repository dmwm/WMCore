import threading
import logging
import cPickle
import time
import sys
import os

from WMCore import Configuration
from WMCore.Agent.BaseHandler import BaseHandler
from ProdAgent.Resources.LSF import LSFConfiguration
from PilotMonitor.plugin.Registry import retrieveMonitor
from TQComp.Apis.TQStateApi import TQStateApi

class PilotMonitorHandler(BaseHandler):

    def __init__(self, component):
        #super class init
        BaseHandler.__init__(self, component)
        print self.component.pollInterval
        self.monitorFlag = True
        self.jobavaliableflag = False
        myThread = threading.currentThread() 
        self.logger = myThread.logger

    def __call__(self, event, payload):
        self.logger.debug("PilotMonitorHandler:event:" + str( event ) ) 

        if event == "MonitorPilots":
            self.monitorPilot( payload )

        elif event == "StopMonitor":
            return

    ########################################
    #generate pilot config params and submit
    ########################################			
    def monitorPilot( self, payload ):
        """ 
        __monitorPilot__
        """
        orgPayload = payload
        self.logger.debug("PilotMonitorHandler:monitorPilot")
        payload = cPickle.loads(payload['payload'])
        #site = payload['site'] 
        #submissionMethod = payload['submissionMethod']
	
        try:
            monitor = retrieveMonitor( \
                      self.component.config.PilotMonitorComponent.plugin )
        except:
            self.logger.debug( 'Error in retrieving monitor: %s' % \
              self.component.config.PilotMonitorComponent.plugin )

        jobSubmitted = False
        attempts = 0
        #return 
        try:
            #self.logger.debug('TQAPI: %s' % self.component.tqStateApi.getPilotCountsBySite())
            result = {}
            
            self.logger.debug(payload)
            self.logger.debug('has site: %s' %payload.has_key('site') )

            if ( payload.has_key('site') ): 
                #hasSite = True
                site = payload['site']
                if ( site == 'CERN' ):
                    self.logger.debug('site to Monitor: %s' % site)
                    return
                result = monitor( site, self.component.tqStateApi) 
                self.logger.debug(result)
            else:
                result = monitor.monitorAll(self.component.tqStateApi)
                self.logger.debug('MonitorAllSites: %s' % result)
 
            if ( not result.has_key('Error') ):     
                #loop over result
                for site in result.keys():
                    self.logger.debug('site result: %s' % site)
                    availableStatus = result[site]['availableStatus']
                    availableCount  = result[site]['available']

                    if ( not jobSubmitted and availableStatus is True):
                        msgPayload = cPickle.dumps({'site':site, \
                                  'bulkSize':availableCount, \
                                  'submissionMethod':'LSF'})
                        #print msgPayload 
                        msg={'name':'SubmitPilotJob',\
                             'payload':msgPayload, \
                             'instant':True }

                        self.logger.debug('should Publish SubmitPilotJob %s' % availableCount )
                        self.sendServiceMsg(msg) 

            #time.sleep(120)

            #msg to itself for continuous working 
            msg = {'name':'MonitorPilots', \
                   'payload':orgPayload['payload'], \
                   'instant': False, \
                   'delay': self.component.pollInterval}
            self.sendServiceMsg(msg)
        except:
            self.logger.debug( 'PilotMonitor: Monitor Error %s:%s:%s' % \
                (sys.exc_info()[0], sys.exc_info()[1]), sys.exc_info()[2] ) 


    ###################################
    #method to publish messages
    ###################################
    def sendServiceMsg(self, msg):
        self.logger.debug("sending message from PilotMonitorHandler")
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.msgService.publish(msg)
        myThread.transaction.commit()
        self.logger.debug('sending message from PilotMonitorHandler ends')
