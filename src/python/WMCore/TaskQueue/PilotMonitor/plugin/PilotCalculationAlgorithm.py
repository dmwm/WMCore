#!/usr/bin/env python
"""
_MonitorInterface_

Interface class for Monitor plugins.

Interface is pretty simple:

Override the Call method to return a ResourceConstraint instance,
which is the number of resources available for jobs and constraints.

The PluginConfig mechanism is used for this as well, so you can read
dynamic parameters from self.pluginConfig


"""
import logging
import sys


def runAlgorithm(totalSubmitted, siteThr, tqstateApi=None):
    pjMinThreshold = 0
    pjMaxThreshold = 0
    pilotThreshold = 0
    try:
        pjMinThreshold = siteThr['pilotMinSub']
        pjMaxThreshold = siteThr['pilotMaxSub']
        pilotThreshold = siteThr['pilotThreshold']
    except:
        logging.info('runAlgorithm Error: %s, %s' % \
                     (sys.exc_info()[0], sys.exc_info()[1]))

    logging.debug('pjMinThreshold %s'% pjMinThreshold )
    logging.debug('pjMaxThreshold %s'% pjMaxThreshold )
    logging.debug('pilotThreshold %s'% pilotThreshold )

    available = pilotThreshold - totalSubmitted
    availableStatus = False

    if ( available <= 0 ):
        #do nothing n return
        availableStatus = False

    if ( available < pjMinThreshold ):
        logging.debug('Do NOTHING pjMinThreshold not met')
        availableStatus = False

    elif ( available > pjMaxThreshold ):
          available = pjMaxThreshold
          availableStatus = True
    print available
    print availableStatus 
    logging.info("PJ MaxThreshold: %s" % pjMaxThreshold)
    logging.info("PJ MinThreshold: %s" % pjMinThreshold)
    logging.info("PJ pilotThreshold: %s" % pilotThreshold)
    result = {}
    result['available'] = available
    result['availableStatus'] = availableStatus

    return result 

#a = {'pilotMinSub':1,'pilotMaxSub':10, 'pilotThreshold':20}
#runAlgorithm(12, a) 
