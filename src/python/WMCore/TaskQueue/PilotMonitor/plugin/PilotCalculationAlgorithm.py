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

def cmpf(x, y):
   if not y['sites']: return -1
   if not x['sites']: return 1
   if len(y['sites']) > len(x['sites']): return -1
   if len(y['sites']) < len(x['sites']): return 1
   return 0

def runAlgorithm2(totalSubmitted, siteName, siteThr, siteVals, taskPacks):

    # Calculate how many pilots would go for this site
    logging.debug('runAlgorithm2()')
    logging.debug(siteVals)
    if ( not siteVals.has_key('SubmittedPilots') ):
         siteVals['SubmittedPilots'] = totalSubmitted
 
    available = siteThr['pilotThreshold'] - siteVals['SubmittedPilots']

    # Calculate how mnay pilots are not yet active
    inactive = siteVals['SubmittedPilots'] - siteVals['ActivePilots']

    if available > siteThr['maximumSubmission']:
        # do not submit more than maxSub at once
        # log this in debug appropriately
        available = siteThr['maximumSubmission']

    tosubmit = 0
    for pack in taskPacks:

        # See if the tasks in this pack match this site
        if ( not pack['sites']) or (siteName in pack['sites']):
            if not pack['tasks']:
                continue

            # Needed pilots = Tasks queueing  -  Not-yet-active pilots
            # For each task, we consume part of all of the inactive pilots
            if inactive >= pack['tasks']:
                inactive -= pack['tasks']
                pack['tasks'] = 0
                # If nothing to submit for this task, just go to next one
                continue
            # If still pack['tasks'], submit new pilots
            else:
                pack['tasks'] -= inactive
                inactive = 0

            # ... unless there is no room for them (go to next site)
            if ( available <= 0):
                break

            # Submit pack['tasks'] up to the room for pilots we still have
            new = min(available, pack['tasks'])
            available -= new
            pack['tasks'] -= new
            tosubmit += new


    #nd some more if there is room and not too many inactive already
    if siteVals['IdlePilots'] < siteThr['minIdlePilots']:
        tosubmit += min(available, siteThr['minIdlePilots'] - siteVals['IdlePilots'])
        # Check the inactive again (in case we still haven't consumed it all)
        tosubmit -= min(tosubmit, inactive)

    # Check the minimum for total submitted pilots
    if (siteVals['SubmittedPilots'] + tosubmit) < siteThr['minPilots']:
        tosubmit += min(available, siteThr['minPilots'] - siteVals['SubmittedPilots'])

    if ( tosubmit > 0):
        return {'available': tosubmit, 'availableStatus': True}
    else:
        return {'available': 0, 'availableStatus': False}
 

def runAlgorithm(totalSubmitted, siteThr):
    pjMinThreshold = 0
    pjMaxThreshold = 0
    pilotThreshold = 0
    try:
        pjMinThreshold = siteThr['minimumSubmission']
        pjMaxThreshold = siteThr['maximumSubmission']
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
    #print available
    #print availableStatus 
    logging.info("PJ MaxThreshold: %s" % pjMaxThreshold)
    logging.info("PJ MinThreshold: %s" % pjMinThreshold)
    logging.info("PJ pilotThreshold: %s" % pilotThreshold)

    result = {}
    result['available'] = available
    result['availableStatus'] = availableStatus
    logging.debug('at the end of algo')
    return result 

#a = {'pilotMinSub':1,'pilotMaxSub':10, 'pilotThreshold':20}
#runAlgorithm(12, a) 
