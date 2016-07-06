#!/usr/bin/env python
from __future__ import division

import copy
import json
import logging
import pprint
import sys
import time
from datetime import datetime

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import Document as CouchDoc
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.McM.McM import McM, McMNoDataError
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

maxMCMCalls = 250  # Maximum # of entries to retrieve from McM per run


def gatherWMDataMiningStats(wmstatsUrl, reqmgrUrl, wmMiningUrl,
                            mcmUrl, mcmCert, mcmKey, tmpDir,
                            archived=False, log=logging.info):
    server, database = splitCouchServiceURL(wmMiningUrl)
    analyticsServer = CouchServer(server)
    couchdb = analyticsServer.connectDatabase(database)

    WMStats = WMStatsReader(wmstatsUrl, reqdbURL=reqmgrUrl, reqdbCouchApp="ReqMgr")

    reqMgrServer, reqMgrDB = splitCouchServiceURL(reqmgrUrl)

    reqMgr = CouchServer(reqMgrServer).connectDatabase(reqMgrDB, False)

    if archived:
        funcName = "Archived Requests"
    else:
        funcName = "Active Requests"

    log.info("%s: Getting job information from %s and %s. Please wait." % (
        funcName, wmstatsUrl, reqmgrUrl))

    if archived:
        checkStates = ['normal-archived', 'rejected-archived', 'aborted-archived']
        jobInfoFlag = False
    else:
        checkStates = WMStatsReader.ACTIVE_STATUS
        jobInfoFlag = True
    requests = WMStats.getRequestByStatus(checkStates, jobInfoFlag=jobInfoFlag, legacyFormat=True)

    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    requestsDict = requestCollection.getData()
    log.info("%s: Total %s requests retrieved\n" % (funcName, len(result)))

    report = {}
    nMCMCalls = 0
    with McM(cert=mcmCert, key=mcmKey, url=mcmUrl, tmpDir=tmpDir) as mcm:
        for wf in result.keys():

            # Store a copy of the CouchDB document so we can compare later before updating
            if couchdb.documentExists(wf):
                oldCouchDoc = couchdb.document(wf)
                wfExists = True
            else:
                oldCouchDoc = CouchDoc(id=wf)
                wfExists = False

            newCouchDoc = copy.deepcopy(oldCouchDoc)
            ancientCouchDoc = copy.deepcopy(oldCouchDoc)
            report[wf] = oldCouchDoc
            # FIXME: remove report, only have two instances of couchDoc

            if 'filterEfficiency' not in oldCouchDoc or 'runWhiteList' not in oldCouchDoc:
                runWhiteList = []
                filterEfficiency = None
                try:
                    # log.debug("Looking up %s in ReqMgr" % wf)
                    rmDoc = reqMgr.document(wf)
                    runWhiteList = rmDoc.get('RunWhiteList', [])
                    filterEfficiency = rmDoc.get('FilterEfficiency', None)
                except:
                    pass  # ReqMgr no longer has the workflow
                report[wf].update({'filterEfficiency': filterEfficiency, 'runWhiteList': runWhiteList})

            if oldCouchDoc.get('mcmTotalEvents', 'Unknown') == 'Unknown' or \
                oldCouchDoc.get('mcmApprovalTime', 'Unknown') == 'Unknown':

                prepID = oldCouchDoc.get('prepID', None)
                if prepID and nMCMCalls <= maxMCMCalls:
                    log.info("Trying to update McM info for %s, PREPID %s" % (wf, prepID))
                    # Get information from McM. Don't call too many times, can take a long time
                    nMCMCalls += 1
                    try:
                        mcmHistory = mcm.getHistory(prepID=prepID)
                        if 'mcmApprovalTime' not in oldCouchDoc:
                            report[wf].update({'mcmApprovalTime': 'NoMcMData'})
                        found = False
                        for entry in mcmHistory:
                            if entry['action'] == 'set status' and entry['step'] == 'announced':
                                dateString = entry['updater']['submission_date']
                                dt = datetime.strptime(dateString, '%Y-%m-%d-%H-%M')
                                report[wf].update({'mcmApprovalTime': time.mktime(dt.timetuple())})
                                found = True
                        if not found:
                            log.error("History found but no approval time for %s" % wf)
                    except McMNoDataError:
                        log.error("Setting NoMcMData for %s" % wf)
                        report[wf].update({'mcmApprovalTime': 'NoMcMData'})
                    except (RuntimeError, IOError):
                        exc_type, dummy_exc_value, dummy_exc_traceback = sys.exc_info()
                        log.error("%s getting history from McM for PREP ID %s. May be transient and/or SSO problem." %
                                  (exc_type, prepID))
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        log.error("%s getting history from McM for PREP ID %s. Unknown error." %
                                  (exc_type, prepID))

                    try:
                        mcmRequest = mcm.getRequest(prepID=prepID)
                        report[wf].update({'mcmTotalEvents': mcmRequest.get('total_events', 'NoMcMData')})
                    except (RuntimeError, IOError):
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        log.error("%s getting request from McM for PREP ID %s. May be transient and/or SSO problem." %
                                  (exc_type, prepID))
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        log.error("%s getting request from McM for PREP ID %s. Unknown error." %
                                  (exc_type, prepID))

            # Basic parameters of the workflow
            priority = requests[wf].get('priority', 0)
            requestType = requests[wf].get('request_type', 'Unknown')
            targetLumis = requests[wf].get('input_lumis', 0)
            targetEvents = requests[wf].get('input_events', 0)
            campaign = requests[wf].get('campaign', 'Unknown')
            prep_id = requests[wf].get('prep_id', None)
            outputdatasets = requests[wf].get('outputdatasets', [])
            statuses = requests[wf].get('request_status', [])

            if not statuses:
                log.error("Could not find any status from workflow: %s" % wf)  # Should not happen but it does.

            # Remove a single  task_ from the start of PREP ID if it exists
            if prep_id and prep_id.startswith('task_'):
                prep_id.replace('task_', '', 1)

            # Can be an empty list, full list, empty string, or non-empty string!
            inputdataset = requests[wf].get('inputdataset', "")
            if isinstance(inputdataset, list):
                if inputdataset:
                    inputdataset = inputdataset[0]
                else:
                    inputdataset = ''

            outputTier = 'Unknown'
            try:
                outputTiers = []
                for ds in outputdatasets:
                    if isinstance(ds, list):
                        outputTiers.append(ds[0].split('/')[-1])
                    else:
                        outputTiers.append(ds.split('/')[-1])
            except:
                log.error(
                    "Could not decode outputdatasets: %s" % outputdatasets)  # Sometimes is a list of lists, not just a list. Bail
            if inputdataset:
                inputTier = inputdataset.split('/')[-1]
                if inputTier in ['GEN']:
                    outputTier = 'LHE'
                elif inputTier in ['RAW', 'RECO']:
                    outputTier = 'AOD'
                elif inputTier in ['GEN-SIM']:
                    outputTier = 'AODSIM'
                elif 'AODSIM' in outputTiers:
                    outputTier = 'AODSIM'

            else:
                if len(outputTiers) == 1 and 'GEN' in outputTiers:
                    if 'STEP0ATCERN' in wf:
                        outputTier = 'STEP0'
                    else:
                        outputTier = 'FullGen'
                elif 'GEN-SIM' in outputTiers and 'AODSIM' in outputTiers and requestType == 'TaskChain':
                    outputTier = 'RelVal'
                elif 'RECO' in outputTiers and requestType == 'TaskChain':
                    outputTier = 'RelVal'
                elif 'GEN-SIM' in outputTiers:
                    outputTier = 'GEN-SIM'
                elif 'AODSIM' in outputTiers:
                    outputTier = 'AODSIM'
                elif 'RECO' in outputTiers:
                    outputTier = 'AOD'
                elif 'AOD' in outputTiers:
                    outputTier = 'AOD'
                else:
                    outputTier = 'GEN-SIM'

            # Calculate completion ratios for events and lumi sections, take minimum for all datasets
            eventPercent = 200
            lumiPercent = 200
            datasetReports = requestsDict[wf].getProgressSummaryByOutputDataset()
            for dataset in datasetReports:
                dsr = datasetReports[dataset].getReport()
                events = dsr.get('events', 0)
                lumis = dsr.get('totalLumis', 0)
                if targetLumis:
                    lumiPercent = min(lumiPercent, lumis / targetLumis * 100)
                if targetEvents:
                    eventPercent = min(eventPercent, events / targetEvents * 100)
            if eventPercent > 100:
                eventPercent = 0
            if lumiPercent > 100:
                lumiPercent = 0

            # Sum up all jobs across agents to see if we've run the first, last
            successJobs = 0
            totalJobs = 0
            for agent in result[wf]:
                jobs = result[wf][agent]
                successJobs += jobs['sucess']
                totalJobs += jobs['created']
            try:
                if totalJobs and not report[wf].get('firstJobTime', None):
                    report[wf].update({'firstJobTime': int(time.time())})
                if totalJobs and successJobs == totalJobs and not report[wf].get('lastJobTime', None):
                    report[wf].update({'lastJobTime': int(time.time())})
            except:
                pass

            # Figure out current status of workflow and transition times
            finalStatus = None
            newTime = None
            approvedTime = None
            assignedTime = None
            acquireTime = None
            completedTime = None
            closeoutTime = None
            announcedTime = None
            archivedTime = None
            requestDate = None

            for status in statuses:
                finalStatus = status['status']
                if status['status'] == 'new':
                    newTime = status['update_time']
                if status['status'] == 'assignment-approved':
                    approvedTime = status['update_time']
                if status['status'] == 'assigned':
                    assignedTime = status['update_time']
                if status['status'] == 'completed':
                    completedTime = status['update_time']
                if status['status'] == 'acquired':
                    acquireTime = status['update_time']
                if status['status'] == 'closed-out':
                    closeoutTime = status['update_time']
                if status['status'] == 'announced':
                    announcedTime = status['update_time']
                if status['status'] == 'normal-archived':
                    archivedTime = status['update_time']

            # Build or modify the report dictionary for the WF
            report.setdefault(wf, {})

            if approvedTime and not report[wf].get('approvedTime', None):
                report[wf].update({'approvedTime': approvedTime})
            if assignedTime and not report[wf].get('assignedTime', None):
                report[wf].update({'assignedTime': assignedTime})
            if acquireTime and not report[wf].get('acquireTime', None):
                report[wf].update({'acquireTime': acquireTime})
            if closeoutTime and not report[wf].get('closeoutTime', None):
                report[wf].update({'closeoutTime': closeoutTime})
            if announcedTime and not report[wf].get('announcedTime', None):
                report[wf].update({'announcedTime': announcedTime})
            if completedTime and not report[wf].get('completedTime', None):
                report[wf].update({'completedTime': completedTime})
            if newTime and not report[wf].get('newTime', None):
                report[wf].update({'newTime': newTime})
            if archivedTime and not report[wf].get('archivedTime', None):
                report[wf].update({'archivedTime': archivedTime})

            try:
                dt = requests[wf]['request_date']
                requestDate = '%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d' % tuple(dt)
                report[wf].update({'requestDate': requestDate})
            except:
                pass

            report[wf].update({'priority': priority, 'status': finalStatus, 'type': requestType})
            report[wf].update({'totalLumis': targetLumis, 'totalEvents': targetEvents,})
            report[wf].update({'campaign': campaign, 'prepID': prep_id, 'outputTier': outputTier,})
            report[wf].update({'outputDatasets': outputdatasets, 'inputDataset': inputdataset,})

            report[wf].setdefault('lumiPercents', {})
            report[wf].setdefault('eventPercents', {})
            lumiProgress = 0
            eventProgress = 0
            for percentage in [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 98, 99, 100]:
                percent = str(percentage)
                percentReported = report[wf]['lumiPercents'].get(percent, None)
                if not percentReported and lumiPercent >= percentage:
                    report[wf]['lumiPercents'][percent] = int(time.time())
                if lumiPercent >= percentage:
                    lumiProgress = percentage

                percentReported = report[wf]['eventPercents'].get(percent, None)
                if not percentReported and eventPercent >= percentage:
                    report[wf]['eventPercents'][percent] = int(time.time())
                if eventPercent >= percentage:
                    eventProgress = percentage

            report[wf].update({'eventProgress': eventProgress, 'lumiProgress': lumiProgress,})

            newCouchDoc.update(report[wf])

            # Queue the updated document for addition if it's changed.
            if ancientCouchDoc != newCouchDoc:
                if wfExists:
                    # log.debug("Workflow updated: %s" % wf)
                    pass
                else:
                    # log.debug("Workflow created: %s" % wf)
                    pass

                try:
                    newCouchDoc['updateTime'] = int(time.time())
                    report[wf]['updateTime'] = int(time.time())
                    dummy = json.dumps(newCouchDoc)  # Make sure it encodes before trying to queue
                    couchdb.queue(newCouchDoc)
                except:
                    log.error("Failed to queue document:%s \n" % pprint.pprint(newCouchDoc))

    log.info("%s: Finished getting job. wait for the next Cycle" % funcName)
    # Commit all changes to CouchDB
    couchdb.commit()
