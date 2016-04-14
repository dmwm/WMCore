#!/usr/bin/python

"""
This is the Dashboard API Module for the Worker Node
"""
from __future__ import absolute_import
from __future__ import print_function

from WMCore.Services.Dashboard import apmon
import time
import sys
import os
import traceback
from types import DictType, StringTypes, ListType

#
# Methods for manipulating the apmon instance
#

# Config attributes
APMONUSEURL = False

# Internal attributes
APMONINSTANCE = None
APMONINIT = False

# Monalisa configuration
#APMONCONF = {'dashb-ai-584.cern.ch:8884': {'sys_monitoring' : 0, \
#                                    'general_info'   : 0, \
#                                    'job_monitoring' : 0} }
APMONCONF = {'cms-jobmon.cern.ch:8884': {'sys_monitoring': 0,
                                         'general_info': 0,
                                         'job_monitoring': 0}}



APMONLOGGINGLEVEL = apmon.Logger.ERROR

#
# Method to create a single apmon instance at a time
#
def getApmonInstance(logr=None, apmonServer=None):
    """ Get Apmon instance
    logr can be set to custom logger or to logging level.
        logging levels are defined in Logger.
    apmonServer can point to custom monitoring server"""
    global APMONINSTANCE
    global APMONINIT
    global APMONCONF
    global APMONLOGGINGLEVEL
    if apmonServer:
        APMONCONF = apmonServer
    if logr:
        APMONLOGGINGLEVEL = logr
    if APMONINSTANCE is None and not APMONINIT:
        APMONINIT = True
        if APMONUSEURL:
            apm = None
            print("Creating ApMon with dynamic configuration/url")
            try:
                apm = apmon.ApMon(APMONCONF, APMONLOGGINGLEVEL)
            except Exception as ex:
                print('Got exception %s' % str(ex))
            if apm is not None and not apm.initializedOK():
                print("Setting ApMon to static configuration")
                try:
                    apm.setDestinations(APMONCONF)
                except Exception:
                    apm = None
            APMONINSTANCE = apm
        if APMONINSTANCE is None:
            print("Creating ApMon with static configuration")
            try:
                APMONINSTANCE = apmon.ApMon(APMONCONF, APMONLOGGINGLEVEL)
            except Exception as ex:
                print('Got exception %s' % str(ex))
    return APMONINSTANCE

#
# Method to free the apmon instance
#
def apmonFree():
    """ Stop backgroun threads, close opened sockets """
    global APMONINSTANCE
    global APMONINIT
    if APMONINSTANCE is not None:
        time.sleep(1)
        try:
            APMONINSTANCE.free()
        except Exception:
            pass
        APMONINSTANCE = None
    APMONINIT = False

#
# Method to send params to Monalisa service
#
def apmonSend(taskid, jobid, params, logr=None, apmonServer=None):
    """ Send multiple parameters to apmon server """
    apm = getApmonInstance(logr, apmonServer)
    if apm is not None:
        if not isinstance(params, DictType) and not isinstance(params, ListType):
            params = {'unknown' : '0'}
        if not isinstance(taskid, StringTypes):
            taskid = 'unknown'
        if not isinstance(jobid, StringTypes):
            jobid = 'unknown'
        try:
            apm.sendParameters(taskid, jobid, params)
            return 0
        except Exception:
            pass
    return 1


def logger(msg):
    """ Common method for writing debug information in a file """
    msg = str(msg)
    if not msg.endswith('\n'):
        msg += '\n'
    try:
        fh = open('report.log', 'a')
        fh.write(msg)
        fh.close()
    except Exception:
        pass

#
# Context handling for CLI
#

# Format envvar, context var name, context var default value
contextConf = {'MonitorID'    : ('MonitorID', 'unknown'),
               'MonitorJobID' : ('MonitorJobID', 'unknown')}

#
# Method to return the context
#
def getContext(overload={}):
    if not isinstance(overload, DictType):
        overload = {}
    context = {}
    for paramName in contextConf.keys():
        paramValue = None
        if paramName in overload:
            paramValue = overload[paramName]
        if paramValue is None:
            envVar = contextConf[paramName][0]
            paramValue = os.getenv(envVar)
        if paramValue is None:
            defaultValue = contextConf[paramName][1]
            paramValue = defaultValue
        context[paramName] = paramValue
    return context

#
# Methods to read in the CLI arguments
#
def readArgs(lines):
    argValues = {}
    for line in lines:
        paramName = 'unknown'
        paramValue = 'unknown'
        line = line.strip()
        if line.find('=') != -1:
            split = line.split('=')
            paramName = split[0]
            paramValue = '='.join(split[1:])
        else:
            paramName = line
        if paramName != '':
            argValues[paramName] = paramValue
    return argValues

def filterArgs(argValues):

    contextValues = {}
    paramValues = {}

    for paramName in argValues.keys():
        paramValue = argValues[paramName]
        if paramValue is not None:
            if paramName in contextConf.keys():
                contextValues[paramName] = paramValue
            else:
                paramValues[paramName] = paramValue
        else:
            logger('Bad value for parameter :' + paramName)
    return contextValues, paramValues

#
# SHELL SCRIPT BASED JOB WRAPPER
# Main method for the usage of the report.py script
#
def report(args):
    argValues = readArgs(args)
    contextArgs, paramArgs = filterArgs(argValues)
    context = getContext(contextArgs)
    taskId = context['MonitorID']
    jobId = context['MonitorJobID']
    logger('SENDING with Task:%s Job:%s' % (taskId, jobId))
    logger('params : ' + repr(paramArgs))
    apmonSend(taskId, jobId, paramArgs)
    apmonFree()
    print("Parameters sent to Dashboard.")

#
# PYTHON BASED JOB WRAPPER
# Main class for Dashboard reporting
#
class DashboardAPI:
    def __init__(self, monitorId=None, jobMonitorId=None, lookupUrl=None):
        self.defaultContext = {}
        self.defaultContext['MonitorID'] = monitorId
        self.defaultContext['MonitorJobID'] = jobMonitorId
        # cannot be set from outside
        self.defaultContext['MonitorLookupURL'] = lookupUrl

    def publishValues(self, taskId, jobId, message):
        contextArgs, paramArgs = filterArgs(message)
        if taskId is not None:
            contextArgs['MonitorID'] = taskId
        if jobId is not None:
            contextArgs['MonitorJobID'] = jobId
        for key in contextConf.keys():
            if key not in contextArgs and self.defaultContext[key] is not None:
                contextArgs[key] = self.defaultContext[key]
        context = getContext(contextArgs)
        taskId = context['MonitorID']
        jobId = context['MonitorJobID']
        apmonSend(taskId, jobId, paramArgs)

    def publish(self, **message):
        self.publishValues(None, None, message)

    def sendValues(self, message, jobId=None, taskId=None):
        self.publishValues(taskId, jobId, message)

    def free(self):
        apmonFree()


def parseAd():
    """ Parse classads (CRAB3) """
    fd = open(os.environ['_CONDOR_JOB_AD'])
    jobad = {}
    for adline in fd.readlines():
        info = adline.split(" = ", 1)
        if len(info) != 2:
            continue
        if info[1].startswith('undefined'):
            val = info[1].strip()
        elif info[1].startswith('"'):
            val = info[1].strip()[1:-1]
        else:
            try:
                val = int(info[1].strip())
            except ValueError:
                continue
        jobad[info[0]] = val
    return jobad


def reportFailureToDashboard(exitCode, ad=None):
    """ Report failure to dashboard (CRAB3) """
    if ad is None:
        try:
            ad = parseAd()
        except:
            print("==== ERROR: Unable to parse job's HTCondor ClassAd ====")
            print("Will not report failure to Dashboard")
            print(traceback.format_exc())
            return exitCode
    missattrs = []
    for attr in ['CRAB_ReqName', 'CRAB_Id', 'CRAB_Retry']:
        if attr not in ad:
            missattrs.append(attr)
    if missattrs:
        print("==== ERROR: HTCondor ClassAd is missing the following attributes: %s ====" % missattrs)
        print("Will not report failure to Dashboard")
        return exitCode
    params = {
        'MonitorID': ad['CRAB_ReqName'],
        'MonitorJobID': '%d_https://glidein.cern.ch/%d/%s_%d' % (ad['CRAB_Id'], ad['CRAB_Id'], ad['CRAB_ReqName'].replace("_", ":"), ad['CRAB_Retry']),
        'JobExitCode': exitCode
    }
    print("Dashboard stageout failure parameters: %s" % str(params))
    apmonSend(params['MonitorID'], params['MonitorJobID'], params)
    apmonFree()
    return exitCode

if __name__ == '__main__':
    sys.exit(reportFailureToDashboard(int(sys.argv[1])))

