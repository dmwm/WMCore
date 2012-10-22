#!/usr/bin/python

"""
This is the Dashboard API Module for the Worker Node
"""

from WMCore.Services.Dashboard import apmon
import time
from types import DictType, StringType, ListType

#
# Methods for manipulating the apmon instance
#

# Internal attributes
apmonInstance = None
apmonInit = False

# Monalisa configuration
apmonConf = ["cms-wmagent-job.cern.ch"]

#
# Method to create a single apmon instance at a time
#
def getApmonInstance( logr, apmonServer ):
    global apmonInstance
    global apmonInit
    if apmonInstance is None and not apmonInit :
        apmonInit = True
        if apmonInstance is None :
            try :
                if not apmonServer:
                    apmonInstance = apmon.ApMon(apmonConf, logr) #apmonLoggingLevel)
                else:
                    apmonInstance = apmon.ApMon(apmonServer, logr)
            except Exception, e :
                pass
    return apmonInstance

#
# Method to free the apmon instance
#
def apmonFree() :
    global apmonInstance
    global apmonInit
    if apmonInstance is not None :
        try :
            apmonInstance.free()
        except Exception, e :
            pass
        apmonInstance = None
    apmonInit = False

#
# Method to send params to Monalisa service
#
def apmonSend(taskid, jobid, params, logr, apmonServer) :
    apm = getApmonInstance( logr, apmonServer )
    if apm is not None :
        if not isinstance(params, DictType) and not isinstance(params, ListType) :
            params = {'unknown' : '0'}
        if not isinstance(taskid, StringType) :
            taskid = 'unknown'
        if not isinstance(jobid, StringType) :
            jobid = 'unknown'
        try :
            apm.sendParameters(taskid, jobid, params)
            return 0
        except Exception, e:
            pass
    return 1
