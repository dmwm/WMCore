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
        site = payload['site'] 
        submissionMethod = payload['submissionMethod']
        
	
        try:
            monitor = retrieveMonitor( \
                      self.component.config.PilotMonitorComponent.plugin )
        except:
            self.logger.debug( 'Error in retrieving monitor: %s' % \
              self.component.config.PilotMonitorComponent.plugin )

        jobSubmitted = False
        attempts = 0
        try:
            #self.logger.debug('TQAPI: %s' % self.component.tqStateApi.getPilotCountsBySite())
            result = monitor( site, self.component.tqStateApi) 
            self.logger.debug(result)
            if ( not result.has_key('Error')):     
                availableStatus = result['availableStatus']
                availableCount  = result['available']
                
                if ( not jobSubmitted and availableStatus is True):
                    msgPayload = cPickle.dumps({'site':'CERN', \
                                  'bulkSize':1, \
                                  'submissionMethod':'LSF'})
                    #print msgPayload 
                    msg={'name':'SubmitPilotJob',\
                         'payload':msgPayload, \
                         'instant':True }

                    self.logger.debug('should Publish SubmitPilotJob %s' % availableCount )
                    self.sendServiceMsg(msg) 
                    #jobSubmitted = True
            time.sleep(20)
            #msg to itself for conitnous working 
            #self.logger.debug('org payload %s' % orgPayload['payload'])
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
