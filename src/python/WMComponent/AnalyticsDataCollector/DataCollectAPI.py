"""
Provide functions to collect data and upload data
"""
from __future__ import division
from builtins import object
from future.utils import viewitems

import os
import time
import json
import logging

import WMCore
from WMCore.Agent.Daemon.Details import Details
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DAOFactory import DAOFactory
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.Services.FWJRDB.FWJRDBAPI import FWJRDBAPI
from WMComponent.AnalyticsDataCollector.DataCollectorEmulatorSwitch import emulatorHook


def threadsDetails(component, pid, downProcessThreads):
    """
    Helper function to provide information about down component in dictionary
    data-format used by agentInfo
    :param component: name of the component
    :param pid: pid of the component process
    :param downProcessThreads: is a list of process thread dictionaries
    :return: dictionary used by down_component_detail part of agentInfo

    NOTE: we preserve data-structure used in down_component_detail which comes from
    WMCore/Agent/Database/MySQL/GetAllHeartbeatInfo.py SQL query and only fill
    out necessary information about component threads and nothing else.
    """
    data = {
            "name": component,
            "pid": pid,
            "worker_name": None,
            "state": "Lost threads",
            "last_updated": int(time.time()),
            "update_threshold": None,
            "poll_interval": None,
            "cycle_time": None,
            "outcome": None,
            "last_error": None,
            "error_message": f"Lost threads: {downProcessThreads}"
          }
    return data

@emulatorHook
class LocalCouchDBData(object):
    def __init__(self, couchURL, statSummaryDB, summaryLevel):
        # set the connection for local couchDB call
        self.couchURL = couchURL
        self.couchURLBase, self.dbName = splitCouchServiceURL(couchURL)
        self.jobCouchDB = CouchServer(self.couchURLBase).connectDatabase(self.dbName + "/jobs", False)
        fwjrDBname = "%s/fwjrs" % self.dbName
        self.fwjrAPI = FWJRDBAPI(self.couchURLBase, fwjrDBname)
        self.summaryStatsDB = CouchServer(self.couchURLBase).connectDatabase(statSummaryDB, False)
        self.summaryLevel = summaryLevel

    def getJobSummaryByWorkflowAndSite(self):
        """
        gets the job status information by workflow

        example
        {"rows":[

            {"key":['request_name1", 'task_name1', "queued_first", "siteA"],"value":100},
            {"key":['request_name1", 'task_name1', "queued_first", "siteB"],"value":100},
            {"key":['request_name1", 'task_name2', "running", "siteA"],"value":100},
            {"key":['request_name1", 'task_name2', "success", "siteB"],"value":100}\
         ]}
         and convert to
         {'request_name1': {'queue_first': { 'siteA': 100}}
          'request_name1': {'queue_first': { 'siteB': 100}}
         }
         if taskflag is set,
         convert to
         {'request_name1': {'tasks': {'task_name1 : {'queue_first': { 'siteA': 100}}}}
          'request_name1': {'tasks':{'task_name1 : {'queue_first': { 'siteB': 100}}}},
          'request_name1': {'tasks':{'task_name2 : {'running': { 'siteA': 100}}}}
          'request_name1': {'tasks':{'task_name2 : {'success': { 'siteB': 100}}}},
         }
        """
        options = {"group": True, "stale": "ok"}
        # site of data should be relatively small (~1M) for put in the memory
        # If not, find a way to stream
        results = self.jobCouchDB.loadView("JobDump", "jobStatusByWorkflowAndSite",
                                           options)

        # reformat the doc to upload to reqmon db
        data = {}
        if self.summaryLevel == "task":
            for x in results.get('rows', []):
                data.setdefault(x['key'][0], {})
                data[x['key'][0]].setdefault('tasks', {})
                data[x['key'][0]]['tasks'].setdefault(x['key'][1], {})
                data[x['key'][0]]['tasks'][x['key'][1]].setdefault(x['key'][2], {})
                data[x['key'][0]]['tasks'][x['key'][1]][x['key'][2]][x['key'][3]] = x['value']
        else:
            for x in results.get('rows', []):
                data.setdefault(x['key'][0], {})
                data[x['key'][0]].setdefault(x['key'][2], {})
                # data[x['key'][0]][x['key'][1]].setdefault(x['key'][2], {})
                data[x['key'][0]][x['key'][2]][x['key'][3]] = x['value']
        logging.info("Found %i requests", len(data))
        return data

    def getJobPerformanceByTaskAndSiteFromSummaryDB(self):

        options = {"include_docs": True}
        results = self.summaryStatsDB.allDocs(options)
        data = {}
        for row in results['rows']:
            if not row['id'].startswith("_"):
                data[row['id']] = {}
                data[row['id']]['tasks'] = row['doc']['tasks']
        return data

    def getEventSummaryByWorkflow(self):
        """
        gets the job status information by workflow

        example
        {"rows":[
            {"key":['request_name1", "/test/output_dataset1"],
             "value": {size: 20286644784714, events: 38938099, count: 6319,
                       dataset: "/test/output_dataset1"}},
            {"key":['request_name1", "/test/output_dataset2"],
             "value": {size: 20286644784714, events: 38938099, count: 6319,
                       dataset: "/test/output_dataset2"}},
            {"key":['request_name1", "/test/output_dataset3"],
             "value": {size: 20286644784714, events: 38938099, count: 6319,
                       dataset: "/test/output_dataset3"}},
            {"key":['request_name1", "/test/output_dataset4"],
             "value": {size: 20286644784714, events: 38938099, count: 6319,
                       dataset: "/test/output_dataset4"}},
         ]}
         and convert to
         {'request_name1': {'size_event': [{size: 20286644784714, events: 38938099, count: 6319,
                             dataset: "/test/output_dataset1"},
                             {size: 20286644784714, events: 38938099, count: 6319,
                             dataset: "/test/output_dataset2"}]}

          'request_name2': ...
        """
        results = self.fwjrAPI.outputByWorkflowName()

        # reformat the doc to upload to reqmon db
        data = {}
        for x in results.get('rows', []):
            data.setdefault(x['key'][0], [])
            data[x['key'][0]].append(x['value'])
        logging.info("Found %i requests", len(data))
        return data

    def getHeartbeat(self):
        try:
            return self.jobCouchDB.info()
        except Exception as ex:
            return {'error_message': str(ex)}

    def getSkippedFilesSummaryByWorkflow(self):
        """
        get skipped file summary
        gets the data with following format
        {u'rows': [{u'value': {u'skippedFile': 5}, u'key':
        ["sryu_StepChain_MC_reqmgr2_170609_180852_5295", "/sryu_StepChain_MC_reqmgr2_170609_180852_5295/GENSIM/GENSIMMergeRAWSIMoutput", "T1_US_FNAL_Disk"]}]}

        and covert to
        {'sryu_TaskChain_Data_wq_testt_160204_061048_5587':
         {'tasks': {'/sryu_TaskChain_Data_wq_testt_160204_061048_5587/RECOCOSD :
                      {'skippedFiles':2}}}}
        """
        results = self.fwjrAPI.getFWJRWithSkippedFiles()
        # reformat the doc to upload to reqmon db
        data = {}
        for x in results.get('rows', []):
            data.setdefault(x['key'][0], {})
            data[x['key'][0]].setdefault('tasks', {})
            data[x['key'][0]]['tasks'].setdefault(x['key'][1], {})
            data[x['key'][0]]['tasks'][x['key'][1]][x['key'][2]] = x['value']
            data[x['key'][0]]['skipped'] = True

        return data


@emulatorHook
class WMAgentDBData(object):
    def __init__(self, summaryLevel, dbi, logger):
        self.summaryLevel = summaryLevel
        # interface to WMBS/BossAir db
        bossAirDAOFactory = DAOFactory(package="WMCore.BossAir",
                                       logger=logger, dbinterface=dbi)
        wmbsDAOFactory = DAOFactory(package="WMCore.WMBS",
                                    logger=logger, dbinterface=dbi)
        wmAgentDAOFactory = DAOFactory(package="WMCore.Agent.Database",
                                       logger=logger, dbinterface=dbi)

        if self.summaryLevel == "task":
            self.batchJobAction = bossAirDAOFactory(classname="JobStatusByTaskAndSite")
        else:
            self.batchJobAction = bossAirDAOFactory(classname="JobStatusByWorkflowAndSite")
        self.runJobByStatus = bossAirDAOFactory(classname="RunJobByStatus")

        self.jobSlotAction = wmbsDAOFactory(classname="Locations.GetJobSlotsByCMSName")
        self.finishedTaskAndJobType = wmbsDAOFactory(classname="Subscriptions.CountFinishedSubscriptionsByTask")
        self.jobCountByState = wmbsDAOFactory(classname="Monitoring.JobCountByState")
        self.jobTypeCountByStatus = wmbsDAOFactory(classname="Monitoring.JobTypeCountByState")
        self.componentStatusAction = wmAgentDAOFactory(classname="GetAllHeartbeatInfo")
        self.listWorkers = wmAgentDAOFactory(classname="MonitorWorkers")

    def getAgentMonitoring(self):
        """
        Return a list of dicts with all information we can gather from the databases.
        """
        monitoring = {}
        monitoring['wmbsCreatedTypeCount'] = self.jobTypeCountByStatus.execute('created')
        monitoring['wmbsExecutingTypeCount'] = self.jobTypeCountByStatus.execute('executing')
        monitoring['wmbsCountByState'] = self.jobCountByState.execute()

        runJobs = self.runJobByStatus.execute()
        monitoring['activeRunJobByStatus'] = runJobs['active']
        monitoring['completeRunJobByStatus'] = runJobs['completed']

        # get thresholds for job creation (GQ to LQ), only for sites in Normal state
        # also get the number of pending jobs and their priority per site
        thresholdsForCreate, pendingCountByPrio = freeSlots(minusRunning=True)
        monitoring['thresholdsGQ2LQ'] = thresholdsForCreate
        monitoring['sitePendCountByPrio'] = pendingCountByPrio
        return monitoring

    def getHeartbeatWarning(self):
        """
        _getHeartbeatWarning_

        Fetch the status and last hearbeat for every single component
        and their threads.

        :return: returns a dictionary with a summary of the component
        status and a short error message, if any.
        """

        results = self.componentStatusAction.execute()
        currentTime = time.time()
        agentInfo = {}
        agentInfo['status'] = 'ok'
        agentInfo['down_components'] = []
        agentInfo['down_component_detail'] = []

        for componentInfo in results:
            noHeartbeat = (currentTime - componentInfo["last_updated"]) > componentInfo["update_threshold"]
            if componentInfo["state"] == "Error" or noHeartbeat:
                agentInfo['status'] = 'down'
                agentInfo['down_components'].append(componentInfo['name'])
                if componentInfo["state"] == "Running":
                    componentInfo["state"] = "Timeout"
                    lastHeartbeat = (currentTime - componentInfo["last_updated"]) // 60
                    componentInfo["error_message"] = "Last worker thread heartbeat was %d min ago" % lastHeartbeat
                agentInfo['down_component_detail'].append(componentInfo)

        return agentInfo

    def getComponentStatus(self, config):
        """
        _getComponentStatus_

        Aggregates the output of getHeartbeatWarning method with the Daemon
        checks performed.
        :param config: agent configuration object
        :return: returns a dictionary with a summary of the component
        status and a short error message, if any.
        """
        agentInfo = self.getHeartbeatWarning()

        components = config.listComponents_() + config.listWebapps_()
        # check the component status
        agentComponents = {}
        for component in components:
            compDir = config.section_(component).componentDir
            compDir = os.path.expandvars(compDir)
            daemonXml = os.path.join(compDir, "Daemon.xml")
            downFlag = False
            if not os.path.exists(daemonXml):
                downFlag = True
            else:
                daemon = Details(daemonXml)
                if not daemon.isAlive():
                    downFlag = True
                # add individual component thread status
                compName = compDir.split('/')[-1]
                compProcessStatus = daemon.processStatus()
                agentComponents.update({compName: compProcessStatus})
                # check if number of component threads is equal to initial set
                cpath = os.path.join(compDir, "threads.json")
                downProcessThreads = []
                if os.path.exists(cpath):
                    origThreads = []
                    with open(cpath, 'r', encoding='utf-8') as istream:
                        origThreads = json.load(istream)
                    if len(origThreads) != len(compProcessStatus):
                        downFlag = True
                        for proc in origThreads:
                            if proc not in compProcessStatus:
                                downProcessThreads.append(proc)
                # check if all component process' threads are alive, otherwise set down flag
                for proc in compProcessStatus:
                    # the alive process should be either in sleeping or running states
                    if proc['status'] not in ["S (sleeping)", "R (running)"]:
                        downFlag = True
                        downProcessThreads.append(proc)
            if downFlag and component not in agentInfo['down_components']:
                agentInfo['status'] = 'down'
                agentInfo['down_components'].append(component)
                if len(downProcessThreads) > 0:
                    agentInfo['down_component_detail'].append(
                            threadsDetails(component, daemon['ProcessID'], downProcessThreads))
                else:
                    agentInfo['down_component_detail'].append(component)

        agentInfo['components'] = agentComponents
        agentInfo['workers'] = self.listWorkers.execute()
        return agentInfo

    def getBatchJobInfo(self):
        return self.batchJobAction.execute()

    def getJobSlotInfo(self):
        return self.jobSlotAction.execute()

    def getFinishedSubscriptionByTask(self):
        results = self.finishedTaskAndJobType.execute()
        finishedSubs = {}
        for item in results:
            finishedSubs.setdefault(item['workflow'], {})
            finishedSubs[item['workflow']].setdefault(item['task'], {})
            finishedSubs[item['workflow']].setdefault('tasks', {})
            # Assumption: task has only one job type.
            finishedSubs[item['workflow']]['tasks'][item['task']] = {}
            finishedSubs[item['workflow']]['tasks'][item['task']]['jobtype'] = item['jobtype']
            finishedSubs[item['workflow']]['tasks'][item['task']]['subscription_status'] = {}
            finishedSubs[item['workflow']]['tasks'][item['task']]['subscription_status']['finished'] = item['finished']
            finishedSubs[item['workflow']]['tasks'][item['task']]['subscription_status']['open'] = item['open']
            finishedSubs[item['workflow']]['tasks'][item['task']]['subscription_status']['total'] = item['total']
            finishedSubs[item['workflow']]['tasks'][item['task']]['subscription_status']['updated'] = item['updated']
        return finishedSubs

def combineAnalyticsData(a, b, combineFunc=None):
    """
        combining 2 data which is the format of dict of dict of ...
        a = {'a' : {'b': {'c': 1}}, 'b' : {'b': {'c': 1}}}
        b = {'a' : {'b': {'d': 1}}}
        should return
        result = {'a' : {'b': {'c': 1, 'd': 1}}, 'b' : {'b': {'c': 1}}}

        result is not deep copy
        when combineFunc is specified, if one to the values are not dict type,
        it will try to combine two values.
        i.e. combineFunc = lambda x, y: x + y
    """
    result = {}
    result.update(a)
    for key, value in viewitems(b):
        if key not in result:
            result[key] = value
        else:
            if not combineFunc and (not isinstance(value, dict) or not isinstance(result[key], dict)):
                # this will raise error if it can't combine two
                result[key] = combineFunc(value, result[key])
            else:
                result[key] = combineAnalyticsData(value, result[key])
    return result


def convertToRequestCouchDoc(combinedRequests, fwjrInfo, finishedTasks,
                             skippedInfoFromCouch, agentInfo,
                             uploadTime, summaryLevel):
    requestDocs = []
    for request, status in viewitems(combinedRequests):
        doc = {}
        doc['_id'] = '%s-%s' % (agentInfo['agent_url'], request)
        doc.update(agentInfo)
        doc['type'] = "agent_request"
        doc['workflow'] = request
        doc['status'] = {}
        doc['sites'] = {}

        if request in skippedInfoFromCouch:
            doc['skipped'] = skippedInfoFromCouch[request]['skipped']
        else:
            doc['skipped'] = False
        # this will set doc['status'], and doc['sites']
        if summaryLevel == 'task':
            if 'tasks' in status:
                tempData = _convertToStatusSiteFormat(status['tasks'], summaryLevel)
                doc['tasks'] = tempData["tasks"]
                doc['status'] = tempData['status']
                doc['sites'] = tempData['sites']
                if doc['skipped']:
                    for task in skippedInfoFromCouch[request]['tasks']:
                        doc['tasks'].setdefault(task, {"skipped": {}})
                        doc['tasks'][task]["skipped"] = skippedInfoFromCouch[request]['tasks'][task]

            # TODO need to handle this correctly by task
            if 'inWMBS' in status:
                doc['status']['inWMBS'] = status['inWMBS']
            if 'inQueue' in status:
                doc['status']['inQueue'] = status['inQueue']
        else:
            tempData = _convertToStatusSiteFormat(status, summaryLevel)
            doc['status'] = tempData['status']
            doc['sites'] = tempData['sites']

        doc['timestamp'] = uploadTime
        # doc['output_progress'] = fwjrInfo.get(request, [])
        # if task is not specified set default
        doc.setdefault('tasks', {})
        if request in fwjrInfo:
            doc['tasks'] = combineAnalyticsData(doc['tasks'], fwjrInfo[request]['tasks'])
        if request in finishedTasks:
            doc['tasks'] = combineAnalyticsData(doc['tasks'], finishedTasks[request]['tasks'])
        requestDocs.append(doc)
    return requestDocs


def _setMultiLevelStatus(statusData, status, value):
    """
    handle the sub status structure
    (i.e. submitted_pending, submitted_running -> {submitted: {pending: , running:}})
    prerequisite: status structure is seperated by '_'
    Currently handle only upto 2 level stucture but can be extended
    """
    statusStruct = status.split('_')
    if len(statusStruct) == 1:
        statusData.setdefault(status, 0)
        statusData[status] += value
    else:
        # only assumes len is 2, can be extended
        statusData.setdefault(statusStruct[0], {})
        statusData[statusStruct[0]].setdefault(statusStruct[1], 0)
        statusData[statusStruct[0]][statusStruct[1]] += value
    return


def _combineJobsForStatusAndSite(requestData, data):
    for status, siteJob in viewitems(requestData):
        if not isinstance(siteJob, dict):
            _setMultiLevelStatus(data['status'], status, siteJob)
        else:
            for site, job in viewitems(siteJob):
                _setMultiLevelStatus(data['status'], status, int(job))
                if site != 'Agent':
                    if site is None:
                        site = 'unknown'
                    data['sites'].setdefault(site, {})
                    _setMultiLevelStatus(data['sites'][site], status, int(job))
    return


def _convertToStatusSiteFormat(requestData, summaryLevel=None):
    """
    convert data structure for couch db.
    "status": { "inWMBS": 100, "success": 1000, "inQueue": 100, "cooloff": 1000,
                "submitted": {"retry": 200, "running": 200, "pending": 200, "first": 200"},
                "failure": {"exception": 1000, "create": 1000, "submit": 1000},
                "queued": {"retry": 1000, "first": 1000}},
   "sites": {
       "T1_test-site-1": {"submitted": {"retry": 200, "running": 200, "pending": 200, "first": 200"},
                          "failure": {"exception": 100, "create": 10, "submit": 10},
                          "cooloff": 100, ...}
       },
    """
    data = {}
    data['status'] = {}
    data['sites'] = {}

    if summaryLevel is not None and summaryLevel == 'task':
        data['tasks'] = {}
        for task, taskData in viewitems(requestData):
            data['tasks'][task] = _convertToStatusSiteFormat(taskData)
            _combineJobsForStatusAndSite(taskData, data)
    else:
        _combineJobsForStatusAndSite(requestData, data)
    return data


def _getCouchACDCHtmlBase(acdcCouchURL):
    """
    TODO: currently it is hard code to the front page of ACDC
    When there is more information is available, it can be added
    through
    """

    return '%s/_design/ACDC/collections.html' % sanitizeURL(acdcCouchURL)['url']


def initAgentInfo(config):
    agentInfo = {}
    agentInfo['agent_team'] = config.Agent.teamName
    agentInfo['agent_version'] = WMCore.__version__
    agentInfo['agent'] = config.Agent.agentName
    # temporarly add port for the split test
    agentInfo['agent_url'] = "%s" % config.Agent.hostName
    return agentInfo
