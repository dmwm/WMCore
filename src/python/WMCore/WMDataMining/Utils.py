#!/usr/bin/env python
from __future__ import division

import cjson
import copy
import pprint
import time
import logging

from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import Document as CouchDoc
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection


def gatherWMDataMiningStats(wmstatsUrl, reqmgrUrl, wmminigUrl, archived = False, log = logging.info):
    
    server, database = splitCouchServiceURL(wmminigUrl)
    analyticsServer = CouchServer(server)
    couchdb = analyticsServer.connectDatabase(database)

    WMStats = WMStatsReader(wmstatsUrl)
    
    reqMgrServer, reqMgrDB = splitCouchServiceURL(reqmgrUrl)
    
    reqMgr = CouchServer(reqMgrServer).connectDatabase(reqMgrDB, False)
    
    if archived:
        funcName = "Archived Requests"
    else:
        funcName = "Active Requests"
    
    log("INFO: %s: Getting job information from %s and %s. Please wait." % (
                  funcName, wmstatsUrl, reqmgrUrl))

    if archived:
        checkStates = ['normal-archived', 'rejected-archived', 'aborted-archived']
        jobInfoFlag = False
    else:
        checkStates = WMStatsReader.ACTIVE_STATUS
        jobInfoFlag = True
    requests = WMStats.getRequestByStatus(checkStates, jobInfoFlag = jobInfoFlag)

    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    requestsDict = requestCollection.getData()
    log("INFO: %s: Total %s requests retrieved\n" % (funcName, len(result)))

    report = {}
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

        if not oldCouchDoc.has_key('filterEfficiency') or not oldCouchDoc.has_key('runWhiteList'):
            runWhiteList = []
            filterEfficiency = None
            try:
                #log("DEBUG: Looking up %s in ReqMgr" % wf)
                rmDoc = reqMgr.document(wf)
                runWhiteList = rmDoc.get('RunWhiteList', [])
                filterEfficiency = rmDoc.get('FilterEfficiency', None)
            except:
                pass # ReqMgr no longer has the workflow
            report[wf].update({'filterEfficiency':filterEfficiency, 'runWhiteList':runWhiteList})

        # Basic parameters of the workflow
        priority = requests[wf]['priority']
        requestType = requests[wf]['request_type']
        targetLumis = requests[wf].get('input_lumis', 0)
        targetEvents = requests[wf].get('input_events', 0)
        campaign = requests[wf]['campaign']
        prep_id = requests[wf].get('prep_id', None)
        outputdatasets = requests[wf].get('outputdatasets', [])

        # Can be an empty list, full list, empty string, or non-empty string!
        inputdataset = requests[wf]['inputdataset']
        if isinstance(inputdataset, (list,)):
            if inputdataset:
                inputdataset = inputdataset[0]
            else:
                inputdataset = ''

        outputTier = 'Unknown'
        try:
            outputTiers = [ds.split('/')[-1] for ds in outputdatasets]
        except:
            log("ERROR: Could not decode outputdatasets: %s" % outputdatasets) # Sometimes is a list of lists, not just a list. Bail
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
                lumiPercent = min(lumiPercent, lumis/targetLumis*100)
            if targetEvents:
                eventPercent = min(eventPercent, events/targetEvents*100)
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
                report[wf].update({'firstJobTime' : int(time.time())})
            if totalJobs and successJobs == totalJobs and not report[wf].get('lastJobTime', None):
                report[wf].update({'lastJobTime' : int(time.time())})
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
        for status in requests[wf]['request_status']:
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
            report[wf].update({'approvedTime':approvedTime})
        if assignedTime and not report[wf].get('assignedTime', None):
            report[wf].update({'assignedTime':assignedTime})
        if acquireTime and not report[wf].get('acquireTime', None):
            report[wf].update({'acquireTime':acquireTime})
        if closeoutTime and not report[wf].get('closeoutTime', None):
            report[wf].update({'closeoutTime':closeoutTime})
        if announcedTime and not report[wf].get('announcedTime', None):
            report[wf].update({'announcedTime':announcedTime})
        if completedTime and not report[wf].get('completedTime', None):
            report[wf].update({'completedTime':completedTime})
        if newTime and not report[wf].get('newTime', None):
            report[wf].update({'newTime':newTime})
        if archivedTime and not report[wf].get('archivedTime', None):
            report[wf].update({'archivedTime':archivedTime})

        try:
            dt = requests[wf]['request_date']
            requestDate = '%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d' % tuple(dt)
            report[wf].update({'requestDate' : requestDate})
        except:
            pass

        report[wf].update({'priority':priority, 'status':finalStatus, 'type':requestType})
        report[wf].update({'totalLumis':targetLumis, 'totalEvents':targetEvents, })
        report[wf].update({'campaign' : campaign, 'prepID' : prep_id, 'outputTier' : outputTier, })
        report[wf].update({'outputDatasets' : outputdatasets, 'inputDataset' : inputdataset, })

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

        report[wf].update({'eventProgress' : eventProgress, 'lumiProgress' : lumiProgress,  })

        newCouchDoc.update(report[wf])

        # Queue the updated document for addition if it's changed.
        if ancientCouchDoc != newCouchDoc:
            if wfExists:
                #log("DEBUG: Workflow updated: %s" % wf)
                pass
            else:
                #log("DEBUG Workflow created: %s" % wf)
                pass

            try:
                newCouchDoc['updateTime'] = int(time.time())
                report[wf]['updateTime'] = int(time.time())
                cjson.encode(newCouchDoc) # Make sure it encodes before trying to queue
                couchdb.queue(newCouchDoc)
            except:
                log("ERROR: Failed to queue document:%s \n" % pprint.pprint(newCouchDoc))

    log("INFO: %s: Finished getting job. wait for the next Cycle" % funcName)
    # Commit all changes to CouchDB
    couchdb.commit()
