#!/usr/bin/env python

"""
_PilotClient_

Act as bootstrap for pilot job

"""

__revision__ = "$Id: PilotClient.py,v 1.5 2009/09/16 12:37:43 khawar Exp $"
__version__ = "$Revision: 1.5 $"
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
from PilotJob import getScramInfo,getCMSSWInfo,parseJobSpec, parseJR
"""
pilotConfig = {'pilotID':125, 'pilotName':'Pilot_125', \
               'serverMode':False, 'serverPort':10, \
               'tqaddress':'vocms13.cern.ch:8030', 'TTL':None }

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

if pilotConfig!= None or pilotConfig!='':
    #now decode it in the form which is required
    decodedConfig = decodestring( pilotConfig )
    pilotConfig = cPickle.loads( decodedConfig )

print '2%s'%pilotConfig

#print sys.path
pilotJob = PilotJob(pilotConfig)
pilotJob.startPilot()
#pilotJob.addToDataCache('Pilot','simplejson','/data/khawar/antonio/tqcode/reports/FrameworkJobReport.xml_4')
#parseJobSpec('/data/khawar/antonio/tqcode/specs/jobspec.xml')

#print parseJR('FrameworkJobReport.xml')
#pilotJob.shutdown()
