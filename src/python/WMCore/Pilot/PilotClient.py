#!/usr/bin/env python

"""
_JobQueueComponent_



"""

__revision__ = "$Id: PilotClient.py,v 1.1 2009/05/27 17:20:25 khawar Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Khawar.Ahmad@cern.ch"

import os
import time
import inspect
import getopt
import sys
import cPickle
from base64 import encodestring, decodestring 

#for logging
import logging

from PilotJob import PilotJob

pilotConfig = {'pilotID':125, 'pilotName':'Pilot_125', \
               'serverMode':False, 'serverPort':10, \
               'tqaddress':'vocms13.cern.ch:8030', 'TTL':-1 }

pilotConfig = cPickle.dumps( pilotConfig )
pilotConfig = encodestring( pilotConfig )
print pilotConfig

"""
valid = ['pconfig=','jobdir']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)

pilotConfig = ''
for opt, arg in opts:
    print opt
    print arg
    if opt=='--pconfig':
       pilotConfig = arg
#pilotConfig = pilotConfig.replace(pilotConfig," ")
"""
if pilotConfig!= None or pilotConfig!='':
    #now decode it in the form which is required
    decodedConfig = decodestring( pilotConfig )
    pilotConfig = cPickle.loads( decodedConfig )

print '2%s'%pilotConfig

pilotJob = PilotJob(pilotConfig)
pilotJob.startPilot()
