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
        self.apmon = None

    def __enter__(self):
        self.apmon = self._getApMonInstance()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.apmon.free()

    def _getApMonInstance(self):
        """
        _getApMonInstance_

        Private method that returns an instance of ApMon
        """
        self.logger.debug("Creating ApMon with static configuration")
        apMonConf = {self.server: self.defaultParams}
        try:
            return apmon.ApMon(apMonConf, self.logger)
        except Exception as ex:
            msg = "Error while getting an instance of ApMon under: %s\n" % self.server
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

        if not self.apmon:
            return

        for job in params:
            try:
                # special case for TaskMeta report type
                if job.get('MessageType') == 'TaskMeta':
                    self.apmon.sendParameters(job['TaskName'], job['JobName'], job)
                elif 'MonitorJobID' in job:
                    self.logger.debug("Sending info to dashboard for MonitorID: %s", job['MonitorID'])
                    self.apmon.sendParameters(job.pop('MonitorID'), job.pop('MonitorJobID'), job)
                else:
                    self.logger.debug("Sending info to dashboard for jobid: %s", job['jobId'])
                    self.apmon.sendParameters(job['taskId'], job['jobId'], job)
            except Exception as ex:
                msg = "Error sending the following job information to dashboard: %s\n" % job
                msg += str(ex)
                self.logger.error(msg)

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
        if not message:
            return None
        self.publishValues(None, None, message)

    def sendValues(self, message, jobId=None, taskId=None):
        self.publishValues(taskId, jobId, message)


def logger(msg):
    """ Common method for writing debug information in a file """
    msg = str(msg)
    if not msg.endswith('\n'):
        msg += '\n'
    try:
        with open('report.log', 'a') as fh:
            fh.write(msg)
    except Exception:
        pass


#
# Context handling for CLI
#

# Format envvar, context var name, context var default value
contextConf = {'MonitorID': ('MonitorID', 'unknown'),
               'MonitorJobID': ('MonitorJobID', 'unknown')}


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
    argValues = readArgs(args)
    contextArgs, paramArgs = filterArgs(argValues)
    context = getContext(contextArgs)
    taskId = context['MonitorID']
    jobId = context['MonitorJobID']
    logger('SENDING with Task:%s Job:%s' % (taskId, jobId))
    logger('params : ' + repr(paramArgs))
    with DashboardAPI(logr=logging, server=DASHBOARDURL) as dashboardInst:
        dashboardInst.apMonSend(paramArgs)

    print("Parameters sent to Dashboard.")


def parseAd():
    """ Parse classads (CRAB3) """
    with open(os.environ['_CONDOR_JOB_AD']) as fd:
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
        'MonitorJobID': '%s_https://glidein.cern.ch/%s/%s_%s' % (ad['CRAB_Id'], ad['CRAB_Id'],
                                                                 ad['CRAB_ReqName'].replace("_", ":"), ad['CRAB_Retry']),
        'JobExitCode': exitCode
    }
    if stageOutReport:
        params['StageOutReport'] = stageOutReport
    print("Dashboard stageout failure parameters: %s" % str(params))
    with DashboardAPI(logr=logging, server=DASHBOARDURL) as dashboardInst:
        dashboardInst.apMonSend(params)
    return exitCode


def stageoutPolicyReport(fileToStage, pnn, command, stageOutType, stageOutExit):
    """
    Prepare Dashboard report about stageout policies
    This dashboard report will be used for reporting to dashboard and visualize local/fallback/direct
    stageout related issues for prod/analysis jobs.
    """
    tempDict = {}
    tempDict['LFN'] = fileToStage['LFN'] if 'LFN' in fileToStage else None
    tempDict['PNN'] = pnn if pnn else fileToStage['PNN'] if 'PNN' in fileToStage else None
    tempDict['StageOutCommand'] = command if command else fileToStage['command'] if 'command' in fileToStage else None
    tempDict['StageOutType'] = stageOutType
    tempDict['StageOutExit'] = stageOutExit
    fileToStage['StageOutReport'].append(tempDict)
    return fileToStage


if __name__ == '__main__':
    sys.exit(reportFailureToDashboard(int(sys.argv[1])))
