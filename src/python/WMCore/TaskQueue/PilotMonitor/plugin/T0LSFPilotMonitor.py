#!/usr/bin/env python
"""
_T0LSFPilotMonitor_

PilotMonitor plugin for the T0 LSF submission system


"""
__author__= "Khawar.Ahmad@cern.ch"
__version__= "$Revision: 1.1 $"
__revision__= "$Id: T0LSFPilotMonitor.py,v 1.1 2009/07/30 22:30:50 khawar Exp $" 

import logging
import traceback
import sys
import threading
from PilotMonitor.plugin.MonitorInterface import MonitorInterface
from PilotMonitor.plugin.Registry import registerMonitor
from PilotMonitor.plugin.PilotCalculationAlgorithm import runAlgorithm

from ProdAgent.Resources.LSF import LSFConfiguration, LSFInterface

class T0LSFPilotMonitor(MonitorInterface):
    """
    _T0LSFPilotMonitor_

    Monitor to watch the pilot jobs in the LSF Queues at CERN

    """
    def __init__(self):
        MonitorInterface.__init__(self) 
        #it will serve as the minimum threshold
        self.pjMinThreshold = 10
        #maximum
        self.pjMaxThreshold = 20
        myThread = threading.currentThread() 
        self.logger = myThread.logger

    def __call__(self, site, tqStateApi):

        #
        # check that at least on threshold is set
        #
        self.logger.info ('TOLSFPilotMonitor: %s:' % site)
 
        if (len(self.allSites) == 0):
            msg = "ERROR: No Resource Control Entry"
            raise RuntimeError, msg
        
        result = {}

        try:
            siteValues = tqStateApi.getPilotCountsBySite()
            taskPacks  = tqStateApi.countTasksBySeReq()
            self.logger.debug(siteValues)
            self.logger.debug(taskPacks)
            
            siteThr = self.siteThresholds[site]
            #pjMinThreshold = siteThr['pilotMinSub']
            #pjMaxThreshold = siteThr['pilotMaxSub']
            #pilotThreshold = siteThr['pilotThreshold']
 
            command = '/usr/bin/bjobs -a -w -g %s'%LSFConfiguration.getGroup()
            self.logger.debug(command)
            output = LSFInterface.executeBjobs(command)
            #logging.info(output)
            jobList = {}
            jobStatus = ['PEND', 'RUN']
            pendJobCount = 0
            runJobCount = 0
            exitJobCount = 0 
            for line in output.splitlines(False)[1:]:
                linelist = line.rstrip().split()
                #logging.info(linelist)
                #check job name
                if ( not linelist[6].startswith('PilotJob_') ): 
                    continue
                if ( linelist[2] == 'PEND' ):
                    pendJobCount +=1
                elif (linelist[2] == 'RUN' ):
                    runJobCount +=1
                elif (linelist[2] == 'EXIT' ):
                    exitJobCount +=1
                #logging.info("%s:%s"%(linelist[6], linelist[22])) 
                jobList[linelist[0]]=linelist[6]

            self.logger.info('Pending Jobs: %s'% pendJobCount)  
            self.logger.info('Running Jobs: %s'% runJobCount)

            #decision logic goes here
            totalSubmittedPilots = pendJobCount + runJobCount
            result = runAlgorithm(totalSubmittedPilots, siteThr)
            self.logger.info("Total Jobs (RUN+PEND): %s" % \
                         (totalSubmittedPilots) )

            self.logger.info("PJ RequiredJobs: %s" % result['available'])

 
        except Exception, ex:
            # can only happen if bjobs call failed
            # do nothing in this case, next loop will work
            self.logger.info('Error in T0LSFPilotMonitor: %s, %s' % (sys.exc_info()[0], sys.exc_info()[1]) ) 
            self.logger.info(traceback.format_stack() )             
            self.logger.info("Call to bjobs failed, just wait for next loop")
            return {'Error':'ERROR'}

        return result

        
registerMonitor(T0LSFPilotMonitor, T0LSFPilotMonitor.__name__)
'''     
if ( __name__ == '__main__'):
    t = T0LSFPilotMonitor()  
    t('CERN')
'''
