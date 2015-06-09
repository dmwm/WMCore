#!/usr/bin/env python
"""Helper class for RequestManager interaction
"""

from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.ReqMgr.DataStructs import RequestStatus
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.Database.CMSCouch import CouchError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore import Lexicon
import os
import time
import socket
from operator import itemgetter
import traceback
import threading

class WorkQueueReqMgrInterface():
    """Helper class for ReqMgr interaction"""
    def __init__(self, **kwargs):
        if not kwargs.get('logger'):
            import logging
            kwargs['logger'] = logging
        self.logger = kwargs['logger']
        self.reqMgr = RequestManager(kwargs)
        self.reqmgr2Only = kwargs.get("reqmgr2_only", False)
        #this will break all in one test
        self.reqMgr2 = ReqMgr(kwargs.get("reqmgr2_endpoint", None))
        
        centralurl = kwargs.get("central_logdb_url", "")
        identifier = kwargs.get("log_reporter", "")
        
        # set the thread name before creat the log db. 
        # only sets that when it is not set already
        myThread = threading.currentThread()
        if myThread.getName() == "MainThread":
            myThread.setName(self.__class__.__name__)
            
        self.logdb = LogDB(centralurl, identifier, logger=self.logger)
        self.previous_state = {}

    def __call__(self, queue):
        """Synchronize WorkQueue and RequestManager"""
        msg = ''
        try:    # pull in new work
            work = self.queueNewRequests(queue)
            msg += "New Work: %d\n" % work
        except Exception:
            self.logger.exception("Error caught during RequestManager pull")

        try: # get additional open-running work
            extraWork = self.addNewElementsToOpenRequests(queue)
            msg += "Work added: %d\n" % extraWork
        except Exception:
            self.logger.exception("Error caught during RequestManager split")

        try:    # report back to ReqMgr
            uptodate_elements = self.report(queue)
            msg += "Updated ReqMgr status for: %s\n" % ", ".join([x['RequestName'] for x in uptodate_elements])
        except:
            self.logger.exception("Error caught during RequestManager update")
        else:
            try:    # Delete finished requests from WorkQueue
                self.deleteFinishedWork(queue, uptodate_elements)
            except:
                self.logger.exception("Error caught during work deletion")

        queue.backend.recordTaskActivity('reqmgr_sync', msg)

    def queueNewRequests(self, queue):
        """Get requests from regMgr and queue to workqueue"""
        self.logger.info("Contacting Request manager for more work")
        work = 0
        workLoads = []

        if queue.params['DrainMode']:
            self.logger.info('Draining queue: Skip requesting work from ReqMgr')
            return 0

        try:
            workLoads = self.getAvailableRequests(queue.params['Teams'])
        except Exception as ex:
            traceMsg = traceback.format_exc()
            msg = "Error contacting RequestManager: %s" % traceMsg
            self.logger.warning(msg)
            return 0

        for team, reqName, workLoadUrl in workLoads:
#            try:
#                self.reportRequestStatus(reqName, "negotiating")
#            except Exception, ex:
#                self.logger.error("""
#                    Unable to update ReqMgr state to negotiating: %s
#                    Ignoring this request: %s""" % (str(ex), reqName))
#                continue

            try:
                try:
                    Lexicon.couchurl(workLoadUrl)
                except Exception as ex: # can throw many errors e.g. AttributeError, AssertionError etc.
                    # check its not a local file
                    if not os.path.exists(workLoadUrl):
                        error = WorkQueueWMSpecError(None, "Workflow url validation error: %s" % str(ex))
                        raise error

                self.logger.info("Processing request %s at %s" % (reqName, workLoadUrl))
                units = queue.queueWork(workLoadUrl, request = reqName, team = team)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError) as ex:
                # fatal error - report back to ReqMgr
                self.logger.info('Permanent failure processing request "%s": %s' % (reqName, str(ex)))
                self.logger.info("Marking request %s as failed in ReqMgr" % reqName)
                self.reportRequestStatus(reqName, 'Failed', message = str(ex))
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' \
                '\nError: "%s"' % (reqName, str(ex))
                self.logger.info(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except Exception as ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s": will try again later.' \
                '\nSee log for details.\nError: "%s"' % (reqName, str(ex))
                self.logger.exception('Unknown error processing %s' % reqName)
                self.logdb.post(reqName, msg, 'error')
                continue

            try:
                self.reportRequestStatus(reqName, "acquired")
            except Exception as ex:
                self.logger.warning("Unable to update ReqMgr state: %s" % str(ex))
                self.logger.warning('Will try again later')

            self.logger.info('%s units(s) queued for "%s"' % (units, reqName))
            work += units

            self.logger.info("%s element(s) obtained from RequestManager" % work)
        return work


    def report(self, queue):
        """Report queue status to ReqMgr."""
        new_state = {}
        uptodate_elements = []
        now = time.time()

        elements = queue.statusInbox(dictKey = "RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0] # 1 element tuple
            try:
                request = self.reqMgr2.getRequestByNames(ele['RequestName'])[ele['RequestName']]
                if request['RequestStatus'] in ('failed', 'completed', 'announced',
                                                'epic-FAILED', 'closed-out', 'rejected'):
                    # requests can be done in reqmgr but running in workqueue
                    # if request has been closed but agent cleanup actions
                    # haven't been run (or agent has been retired)
                    # Prune out obviously too old ones to avoid build up
                    if queue.params.get('reqmgrCompleteGraceTime', -1) > 0:
                        if (now - float(ele.updatetime)) > queue.params['reqmgrCompleteGraceTime']:
                            # have to check all elements are at least running and are old enough
                            request_elements = queue.statusInbox(WorkflowName = request['RequestName'])
                            if not any([x for x in request_elements if x['Status'] != 'Running' and not x.inEndState()]):
                                last_update = max([float(x.updatetime) for x in request_elements])
                                if (now - last_update) > queue.params['reqmgrCompleteGraceTime']:
                                    self.logger.info("Finishing request %s as it is done in reqmgr" % request['RequestName'])
                                    queue.doneWork(WorkflowName=request['RequestName'])
                                    continue
                    else:
                        pass # assume workqueue status will catch up later
                elif request['RequestStatus'] == 'aborted' or request['RequestStatus'] == 'force-complete':
                    queue.cancelWork(WorkflowName=request['RequestName'])
                # Check consistency of running-open/closed and the element closure status
                elif request['RequestStatus'] == 'running-open' and not ele.get('OpenForNewData', False):
                    self.reportRequestStatus(ele['RequestName'], 'running-closed')
                elif request['RequestStatus'] == 'running-closed' and ele.get('OpenForNewData', False):
                    queue.closeWork(ele['RequestName'])
                # update request status if necessary
                elif ele['Status'] not in self._reqMgrToWorkQueueStatus(request['RequestStatus']):
                    self.reportElement(ele)
                    
                uptodate_elements.append(ele)
            except Exception as ex:
                msg = 'Error talking to ReqMgr about request "%s": %s'
                traceMsg = traceback.format_exc()
                self.logger.error(msg % (ele['RequestName'], traceMsg))

        return uptodate_elements

    def deleteFinishedWork(self, queue, elements):
        """Delete work from queue that is finished in ReqMgr"""
        finished = []
        for element in elements:
            if self._workQueueToReqMgrStatus(element['Status']) in ('aborted', 'failed', 'completed', 'announced',
                                                         'epic-FAILED', 'closed-out', 'rejected') \
                                                         and element.inEndState():
                finished.append(element['RequestName'])
        return queue.deleteWorkflows(*finished)
    
    def _getRequestsByTeamsAndStatus(self, status, teams = []):
        """
        TODO: now it assumes one team per requests - check whether this assumption is correct
        Check whether we actually use the team for this.
        Also switch to byteamandstatus couch call instead of 
        """
        requests = self.reqMgr2.getRequestByStatus(status)
        #Then sort by Team name then sort by Priority
        #https://docs.python.org/2/howto/sorting.html
        if teams and len(teams) > 0:
            results = {}
            for reqName, value in requests.items():
                if value["Teams"][0] in teams:
                    results[reqName] = value
            return results 
        else:
            return requests    
            
    def getAvailableRequests(self, teams):
        """
        Get available requests for the given teams and sort by team and priority
        returns [(team, request_name, request_spec_url)]
        """
        
        tempResults = self._getRequestsByTeamsAndStatus("assigned", teams).values()
        filteredResults = []
        for request in tempResults:
            if "Teams" in request and len(request["Teams"]) == 1:
                filteredResults.append(request)
            else:
                msg = "no team or more than one team (%s) are assigined: %s" % (
                                    request.get("Teams", None), request["RequestName"])
                self.logger.error(msg)
                self.logdb.post(request["RequestName"], msg, 'error')
        filteredResults.sort(key = itemgetter('RequestPriority'), reverse = True)
        filteredResults.sort(key = lambda r: r["Teams"][0])
        
        results = [(x["Teams"][0], x["RequestName"], x["RequestWorkflow"]) for x in filteredResults]
        
        return results

    def reportRequestStatus(self, request, status, message = None):
        """Change state in RequestManager
           Optionally, take a message to append to the request
        """
        if message:
            self.logdb.post(request, str(message), 'info')
        reqmgrStatus = self._workQueueToReqMgrStatus(status)
        if reqmgrStatus: # only send known states
            try:
                # try reqmgr1 call if it fails
                self.reqMgr.reportRequestStatus(request, reqmgrStatus)
            except Exception as ex:
                # try reqmgr2 call
                msg = "%s : reqmgr2 request: %s" % (request, str(ex))
                self.logdb.post(request, msg, 'warning')
                self.reqMgr2.updateRequestStatus(request, reqmgrStatus)                

    def markAcquired(self, request, url = None):
        """Mark request acquired"""
        self.reqMgr.putWorkQueue(request, url)

    def _workQueueToReqMgrStatus(self, status):
        """Map WorkQueue Status to that reported to ReqMgr"""
        statusMapping = {'Acquired' : 'acquired',
                         'Running' : 'running-open',
                         'Failed' : 'failed',
                         'Canceled' : 'aborted',
                         'CancelRequested' : 'aborted',
                         'Done' : 'completed'
                         }
        if status in statusMapping:
            # if wq status passed convert to reqmgr status
            return statusMapping[status]
        elif status in RequestStatus.REQUEST_STATE_LIST:
            # if reqmgr status passed return reqmgr status
            return status
        else:
            # unknown status
            return None

    def _reqMgrToWorkQueueStatus(self, status):
        """Map ReqMgr status to that in a WorkQueue element, it is not a 1-1 relation"""
        statusMapping = {'acquired': ['Acquired'],
                         'running' : ['Running'],
                         'running-open': ['Running'],
                         'running-closed': ['Running'],
                         'failed': ['Failed'],
                         'aborted': ['Canceled', 'CancelRequested'],
                         'force-complete': ['Canceled', 'CancelRequested'],
                         'completed': ['Done']}
        if status in statusMapping:
            return statusMapping[status]
        else:
            return []

    def reportElement(self, element):
        """Report element to ReqMgr"""
        self.reportRequestStatus(element['RequestName'], element['Status'])

    def addNewElementsToOpenRequests(self, queue):
        """Add new elements to open requests which are in running-open state, only works adding new blocks from the input dataset"""
        self.logger.info("Checking Request Manager for open requests and closing old ones")

        # First close any open inbox element which hasn't found anything new in a while
        queue.closeWork()
        self.report(queue)

        work = 0
        requests = []

        # Drain mode, don't pull any work into open requests. They will be closed if the queue stays in drain long enough
        if queue.params['DrainMode']:
            self.logger.info('Draining queue: Skip requesting work from ReqMgr')
            return 0

        try:
            requests = self._getRequestsByTeamsAndStatus("running-open", queue.params['Teams']).keys()
        except Exception as ex:
            traceMsg = traceback.format_exc()
            msg = "Error contacting RequestManager: %s" % traceMsg
            self.logger.warning(msg)
            return 0

        for reqName in requests:
            try:
                self.logger.info("Processing request %s" % (reqName))
                units = queue.addWork(requestName = reqName)
            except (WorkQueueWMSpecError, WorkQueueNoWorkError) as ex:
                # fatal error - but at least it was split the first time. Log and skip.
                msg = 'Error adding further work to request "%s". Will try again later' \
                '\nError: "%s"' % (reqName, str(ex))
                self.logger.info(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except (IOError, socket.error, CouchError, CouchConnectionError) as ex:
                # temporary problem - try again later
                msg = 'Error processing request "%s": will try again later.' \
                '\nError: "%s"' % (reqName, str(ex))
                self.logger.info(msg)
                self.logdb.post(reqName, msg, 'error')
                continue
            except Exception as ex:
                # Log exception as it isnt a communication problem
                msg = 'Error processing request "%s": will try again later.' \
                '\nSee log for details.\nError: "%s"' % (reqName, str(ex))
                self.logger.exception('Unknown error processing %s' % reqName)
                self.logdb.post(reqName, msg, 'error')
                continue

            self.logger.info('%s units(s) queued for "%s"' % (units, reqName))
            work += units

        self.logger.info("%s element(s) added to open requests" % work)
        return work
