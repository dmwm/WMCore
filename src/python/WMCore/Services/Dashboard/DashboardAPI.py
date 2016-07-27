#!/usr/bin/env python

"""
This is the Dashboard API Module for the Worker Node
"""
from __future__ import print_function

import sys
import os
import traceback
import logging
from WMCore.Services.Dashboard import apmon


# AMR still some functions out there that I don't know about their usage
DASHBOARDURL = 'cms-jobmon.cern.ch:8884'

class DashboardAPI(object):
    """
    _DashboardAPI_

    High level interface with apmon
    """
    def __init__(self, monitorId=None, jobMonitorId=None, lookupUrl=None,
                 logr=None, server=None):
        self.defaultContext = {}
        self.defaultContext['MonitorID'] = monitorId
        self.defaultContext['MonitorJobID'] = jobMonitorId
        # cannot be set from outside
        self.defaultContext['MonitorLookupURL'] = lookupUrl

        self.defaultParams = {'sys_monitoring': 0, 'general_info': 0, 'job_monitoring': 0}
        self.server = server if server else DASHBOARDURL
        self.logger = logr if logr else logging.getLogger()

    def getApMonInstance(self):
        """
        _getApMonInstance_

        Return an instance of ApMon
        """
        self.logger.debug("Creating ApMon with static configuration")
        apMonConf = {self.server: self.defaultParams}
        try:
            return apmon.ApMon(apMonConf, self.logger)
        except Exception as ex:
            msg = "Errror while getting an instance of ApMon under: %s\n" % self.server
            msg += str(ex)
            self.logger.error(msg)
        return None

    def apMonSend(self, params=None):
        """
        _apMonSend_

        Get an apmon instance and send all the job parameters to it.
        """
        if not params:
            self.logger.info("ApMon has nothing to send out")
        elif isinstance(params, dict):
            params = [params]
        elif not isinstance(params, list):
            self.logger.error("apMon send received data in the wrong format: %s", type(params))

        apmonInst = self.getApMonInstance()
        if not apmonInst:
            return

        for job in params:
            self.logger.debug("Sending info to dashboard for jobid: %s", job['jobId'])
            try:
                # special case for TaskMeta report type
                if job.get('MessageType') == 'TaskMeta':
                    apmonInst.sendParameters(job['TaskName'], job['JobName'], job)
                elif 'MonitorJobID' in job:
                    apmonInst.sendParameters(job.pop('MonitorID'), job.pop('MonitorJobID'), job)
                else:
                    apmonInst.sendParameters(job['taskId'], job['jobId'], job)
            except Exception as ex:
                msg = "Error sending the following job information to dashboard: %s\n" % job
                msg += str(ex)
                self.logger.error(msg)

        # then close the socket and release apmon resources
        try:
            apmonInst.free()
        except Exception as ex:
            self.logger.error("Failed to free apmon resources.")

    def publishValues(self, taskId, jobId, message):
        """
        Is there anyone really using this method?!?!
        """
        contextArgs, paramArgs = filterArgs(message)
        if taskId is not None:
            contextArgs['MonitorID'] = taskId
        if jobId is not None:
            contextArgs['MonitorJobID'] = jobId
        for key in contextConf.keys():
            if key not in contextArgs and self.defaultContext[key] is not None:
                contextArgs[key] = self.defaultContext[key]
        context = getContext(contextArgs)
        paramArgs['MonitorID'] = context['MonitorID']
        paramArgs['MonitorJobID'] = context['MonitorJobID']
        self.apMonSend(paramArgs)

    def publish(self, **message):
        self.publishValues(None, None, message)

    def sendValues(self, message, jobId=None, taskId=None):
        self.publishValues(taskId, jobId, message)


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
    if not isinstance(overload, dict):
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
    dashboardInst = DashboardAPI(logr=logging, server=DASHBOARDURL)
    argValues = readArgs(args)
    contextArgs, paramArgs = filterArgs(argValues)
    context = getContext(contextArgs)
    taskId = context['MonitorID']
    jobId = context['MonitorJobID']
    logger('SENDING with Task:%s Job:%s' % (taskId, jobId))
    logger('params : ' + repr(paramArgs))
    dashboardInst.apMonSend(paramArgs)
    print("Parameters sent to Dashboard.")


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


def reportFailureToDashboard(exitCode, ad=None, stageOutReport=None):
    """ Report failure to dashboard (CRAB3) """
    dashboardInst = DashboardAPI(logr=logging, server=DASHBOARDURL)
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
    if stageOutReport:
        params['StageOutReport'] = stageOutReport
    print("Dashboard stageout failure parameters: %s" % str(params))
    dashboardInst.apMonSend(params)
    return exitCode

def stageoutPolicyReport(fileToStage, seName, pnn, command, stageOutType, stageOutExit):
    """
    Prepare Dashboard report about stageout policies
    This dashboard report will be used for reporting to dashboard and visualize local/fallback/direct
    stageout related issues for prod/analysis jobs.
    """
    tempDict = {}
    tempDict['LFN'] = fileToStage['LFN'] if 'LFN' in fileToStage else None
    tempDict['SEName'] = seName if seName else fileToStage['SEName'] if 'SEName' in fileToStage else None
    tempDict['PNN'] = pnn if pnn else fileToStage['PNN'] if 'PNN' in fileToStage else None
    tempDict['StageOutCommand'] = command if command else fileToStage['command'] if 'command' in fileToStage else None
    tempDict['StageOutType'] = stageOutType
    tempDict['StageOutExit'] = stageOutExit
    fileToStage['StageOutReport'].append(tempDict)
    return fileToStage


if __name__ == '__main__':
    sys.exit(reportFailureToDashboard(int(sys.argv[1])))

